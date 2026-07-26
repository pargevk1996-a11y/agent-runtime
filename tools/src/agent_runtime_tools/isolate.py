"""Sandboxed code execution.

Generated code never runs in the calling process — it runs in an isolate. The
light :class:`SubprocessIsolate` used in dev/CI enforces wall-clock and CPU
timeouts and an address-space (memory) cap via POSIX rlimits, and applies a
*best-effort* network block. It is deliberately not a hardened security boundary
(user code could work around the soft network guard); production must supply a
heavy isolate (gVisor/Firecracker). The light backend refuses to start when the
environment is production.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import resource
import signal
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

_MB = 1024 * 1024

# Runs in the sandboxed child: soft-disable networking, then exec the user code
# read from the file path in argv[1]. argv[2] is "1" to allow networking.
_WRAPPER = (
    "import sys\n"
    "code_path = sys.argv[1]\n"
    "allow_net = sys.argv[2] == '1'\n"
    "if not allow_net:\n"
    "    import socket\n"
    "    def _blocked(*a, **k):\n"
    "        raise OSError('network disabled in sandbox')\n"
    "    socket.socket = _blocked\n"
    "    socket.create_connection = _blocked\n"
    "with open(code_path, encoding='utf-8') as f:\n"
    "    src = f.read()\n"
    "exec(compile(src, '<sandbox>', 'exec'), {'__name__': '__main__'})\n"
)


class IsolateError(Exception):
    """The isolate could not run the code (setup failure or misconfiguration)."""


@dataclass(frozen=True)
class ExecLimits:
    """Resource limits for one code execution."""

    timeout_seconds: float = 5.0
    memory_mb: int = 256
    allow_network: bool = False


@dataclass(frozen=True)
class ExecResult:
    """The outcome of running code in an isolate."""

    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool


class Isolate(Protocol):
    """Runs code in isolation under resource limits."""

    async def run(self, code: str, limits: ExecLimits) -> ExecResult:
        """Execute ``code``, returning its captured output and status."""
        ...


def _apply_limits(limits: ExecLimits) -> None:
    # Runs in the forked child before exec. Address space caps memory; CPU time
    # is a backstop for the wall-clock timeout against busy loops.
    mem = limits.memory_mb * _MB
    resource.setrlimit(resource.RLIMIT_AS, (mem, mem))
    cpu = math.ceil(limits.timeout_seconds) + 1
    resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))


class SubprocessIsolate:
    """Light isolate: a resource-limited, network-soft-blocked subprocess."""

    def __init__(self, *, environment: str = "dev", python: str | None = None) -> None:
        if environment == "production":
            raise IsolateError("the subprocess isolate must not be used in production")
        self._python = python or sys.executable

    async def run(self, code: str, limits: ExecLimits) -> ExecResult:
        with tempfile.TemporaryDirectory(prefix="ar-isolate-") as tmp:
            code_path = Path(tmp) / "code.py"
            code_path.write_text(code, encoding="utf-8")
            argv = [
                self._python,
                "-I",
                "-B",
                "-c",
                _WRAPPER,
                str(code_path),
                "1" if limits.allow_network else "0",
            ]
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                preexec_fn=lambda: _apply_limits(limits),
                start_new_session=True,
            )
            try:
                out, err = await asyncio.wait_for(
                    proc.communicate(), timeout=limits.timeout_seconds
                )
            except TimeoutError:
                self._kill(proc)
                await proc.wait()
                return ExecResult(
                    stdout="", stderr="execution timed out", exit_code=-1, timed_out=True
                )

            return ExecResult(
                stdout=out.decode(errors="replace"),
                stderr=err.decode(errors="replace"),
                exit_code=proc.returncode if proc.returncode is not None else -1,
                timed_out=False,
            )

    @staticmethod
    def _kill(proc: asyncio.subprocess.Process) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
