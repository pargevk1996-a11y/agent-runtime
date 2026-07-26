"""Payload type registry and version upcasting.

The payload union is *open*: phases add event types over time, so a static
Pydantic discriminated union (which needs a closed set) does not fit. Instead
each ``event_type`` is registered here with its current model, version, and a
chain of upcaster functions ``vN -> vN+1``.

On read, :meth:`EventRegistry.decode` walks a stored row's ``payload_version`` up
to the current version by applying upcasters in order, then validates into the
current model. Stored rows are never rewritten — migration happens in memory.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from pydantic import ValidationError

from agent_runtime.events.envelope import EventPayload
from agent_runtime.events.errors import EventDecodeError, UnknownEventTypeError

# An upcaster takes a payload dict at version N and returns it shaped for N+1.
Upcaster = Callable[[dict[str, object]], dict[str, object]]


@dataclass(frozen=True)
class _Registration:
    model: type[EventPayload]
    version: int
    upcasters: dict[int, Upcaster]  # keyed by source version N (N -> N+1)


class EventRegistry:
    """A mapping of ``event_type`` to its model, version, and upcasters.

    Instances are independent; production uses :data:`default_registry`, while
    tests construct throwaway registries to avoid global state leaking between
    cases.
    """

    def __init__(self) -> None:
        self._by_type: dict[str, _Registration] = {}
        self._type_by_model: dict[type[EventPayload], str] = {}

    def register(
        self,
        event_type: str,
        model: type[EventPayload],
        *,
        version: int = 1,
        upcasters: Mapping[int, Upcaster] | None = None,
    ) -> None:
        """Register a payload type.

        :raises ValueError: if ``event_type`` or ``model`` is already registered,
            or if the upcaster chain has a gap for versions below ``version``.
        """
        if event_type in self._by_type:
            raise ValueError(f"event_type already registered: {event_type!r}")
        if model in self._type_by_model:
            raise ValueError(f"model already registered: {model.__name__}")
        chain = dict(upcasters or {})
        for v in range(1, version):
            if v not in chain:
                raise ValueError(f"missing upcaster {v}->{v + 1} for {event_type!r}")
        self._by_type[event_type] = _Registration(model, version, chain)
        self._type_by_model[model] = event_type

    def event_type_for(self, payload: EventPayload) -> str:
        """Return the registered ``event_type`` for a payload instance."""
        event_type = self._type_by_model.get(type(payload))
        if event_type is None:
            raise UnknownEventTypeError(
                "payload type is not registered",
                context={"model": type(payload).__name__},
            )
        return event_type

    def current_version(self, event_type: str) -> int:
        """Return the current schema version for an ``event_type``."""
        return self._require(event_type).version

    def registered(self) -> list[tuple[str, type[EventPayload], int]]:
        """List ``(event_type, model, version)`` for every registered payload."""
        return [(name, reg.model, reg.version) for name, reg in self._by_type.items()]

    def decode(self, event_type: str, version: int, raw: Mapping[str, object]) -> EventPayload:
        """Upcast a stored payload to the current version and validate it.

        Invariant: the returned payload is always an instance of the currently
        registered model for ``event_type``.
        """
        reg = self._require(event_type)
        if version > reg.version:
            raise EventDecodeError(
                "stored payload_version is newer than the registered model",
                context={"event_type": event_type, "stored": version, "current": reg.version},
            )
        data = dict(raw)
        v = version
        while v < reg.version:
            upcaster = reg.upcasters.get(v)
            if upcaster is None:
                raise EventDecodeError(
                    "missing upcaster in chain",
                    context={"event_type": event_type, "from_version": v},
                )
            data = upcaster(data)
            v += 1
        try:
            return reg.model.model_validate(data)
        except ValidationError as exc:
            raise EventDecodeError(
                "payload failed validation after upcasting",
                context={"event_type": event_type, "errors": exc.errors()},
            ) from exc

    def _require(self, event_type: str) -> _Registration:
        reg = self._by_type.get(event_type)
        if reg is None:
            raise UnknownEventTypeError(
                "no payload type registered", context={"event_type": event_type}
            )
        return reg


# Process-wide registry used by the store in production.
default_registry = EventRegistry()
