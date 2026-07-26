"""Generate docs/events.md from the event registry (run to refresh).

    uv run python docs/generate_events.py

Keeps the event schema reference in lockstep with the code — the doc is derived,
never hand-maintained.
"""

from __future__ import annotations

from pathlib import Path

from agent_runtime.dag.events import register_dag_events
from agent_runtime.events.registry import EventRegistry
from agent_runtime.runs.events import register_run_events
from agent_runtime.tools.events import register_tool_events


def _type_name(annotation: object) -> str:
    return getattr(annotation, "__name__", None) or str(annotation).replace("typing.", "")


def render() -> str:
    registry = EventRegistry()
    register_run_events(registry)
    register_dag_events(registry)
    register_tool_events(registry)

    lines = [
        "# Event schema reference",
        "",
        "_Generated from the event registry by `docs/generate_events.py` — do not edit by hand._",
        "",
        "Every event shares the frozen envelope (identity, ordering, tenancy, causation,",
        "timestamps, versioning); only the payload varies. Payloads below are grouped by",
        "prefix: `run.*` lifecycle, `node.*`/`edge.*` DAG, `tool.*` tool calls.",
        "",
    ]
    for event_type, model, version in sorted(registry.registered()):
        lines.append(f"## `{event_type}` — v{version}")
        lines.append("")
        lines.append(f"Payload model: `{model.__name__}`")
        lines.append("")
        for name, field in model.model_fields.items():
            lines.append(f"- `{name}`: `{_type_name(field.annotation)}`")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    target = Path(__file__).parent / "events.md"
    target.write_text(render(), encoding="utf-8")
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
