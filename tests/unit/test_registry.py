"""Unit tests for the payload registry and version upcasting."""

from __future__ import annotations

import pytest

from agent_runtime.events.envelope import EventPayload
from agent_runtime.events.errors import EventDecodeError, UnknownEventTypeError
from agent_runtime.events.registry import EventRegistry


class _V1(EventPayload):
    a: int


class _V2(EventPayload):
    a: int
    b: int


def _add_b(data: dict[str, object]) -> dict[str, object]:
    return {**data, "b": 0}


def test_register_and_decode_current_version() -> None:
    reg = EventRegistry()
    reg.register("sample", _V1)
    payload = reg.decode("sample", 1, {"a": 5})
    assert isinstance(payload, _V1)
    assert payload.a == 5


def test_decode_upcasts_old_version() -> None:
    reg = EventRegistry()
    reg.register("sample", _V2, version=2, upcasters={1: _add_b})
    payload = reg.decode("sample", 1, {"a": 3})
    assert isinstance(payload, _V2)
    assert (payload.a, payload.b) == (3, 0)


def test_current_version_and_event_type_lookup() -> None:
    reg = EventRegistry()
    reg.register("sample", _V2, version=2, upcasters={1: _add_b})
    assert reg.current_version("sample") == 2
    assert reg.event_type_for(_V2(a=1, b=2)) == "sample"


def test_unknown_event_type_on_decode() -> None:
    reg = EventRegistry()
    with pytest.raises(UnknownEventTypeError):
        reg.decode("nope", 1, {})


def test_unknown_event_type_on_lookup() -> None:
    reg = EventRegistry()
    with pytest.raises(UnknownEventTypeError):
        reg.event_type_for(_V1(a=1))


def test_future_version_rejected() -> None:
    reg = EventRegistry()
    reg.register("sample", _V1)
    with pytest.raises(EventDecodeError):
        reg.decode("sample", 2, {"a": 1})


def test_missing_upcaster_in_chain_rejected() -> None:
    reg = EventRegistry()
    reg.register("sample", _V2, version=2, upcasters={1: _add_b})
    # Stored at v1 but pretend the chain has a gap by decoding a further-back
    # version: register a v3 model whose chain omits 2->3.
    reg2 = EventRegistry()
    with pytest.raises(ValueError, match="missing upcaster"):
        reg2.register("gappy", _V2, version=3, upcasters={1: _add_b})


def test_duplicate_registration_rejected() -> None:
    reg = EventRegistry()
    reg.register("sample", _V1)
    with pytest.raises(ValueError, match="already registered"):
        reg.register("sample", _V2)


def test_invalid_payload_after_upcast_raises_decode_error() -> None:
    reg = EventRegistry()
    reg.register("sample", _V2, version=2, upcasters={1: _add_b})
    # Upcaster runs, but the base data is missing required 'a'.
    with pytest.raises(EventDecodeError):
        reg.decode("sample", 1, {})
