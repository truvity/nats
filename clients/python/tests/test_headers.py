from __future__ import annotations

from truvity_nats import HeaderCarrier, extract_trace, inject_trace

TP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_inject_writes_lower_case_names() -> None:
    h: dict[str, str] = {}
    inject_trace(h, TP, "k=v")
    assert h == {"traceparent": TP, "tracestate": "k=v"}


def test_inject_skips_empty_values_and_replaces_every_spelling() -> None:
    h = {"Traceparent": "old", "TRACEPARENT": "older", "Tracestate": "a=b", "x-other": "1"}
    inject_trace(h, TP, "")
    assert h == {"traceparent": TP, "x-other": "1"}


def test_extract_is_case_insensitive() -> None:
    assert extract_trace({"Traceparent": TP, "Tracestate": "a=b"}) == (TP, "a=b")
    assert extract_trace({"traceparent": TP}) == (TP, "")
    assert extract_trace({"TRACEPARENT": TP, "tracestate": "x=y"}) == (TP, "x=y")
    assert extract_trace({}) == ("", "")
    assert extract_trace(None) == ("", "")


def test_carrier_is_a_case_insensitive_mapping() -> None:
    h = {"Traceparent": TP}
    c = HeaderCarrier(h)
    assert c.get("traceparent") == TP
    assert c["TRACEPARENT"] == TP
    assert c.get("tracestate") is None
    c["TraceState"] = "a=b"
    assert h == {"Traceparent": TP, "tracestate": "a=b"}
    c["traceparent"] = TP
    assert sorted(c.keys()) == ["traceparent", "tracestate"]
    assert "Traceparent" not in h
    c["tracestate"] = ""
    assert "tracestate" not in c
    del c["traceparent"]
    assert not h


def test_carrier_works_as_an_otel_style_carrier() -> None:
    # What a propagator does with a carrier: a default setter writes, a default getter reads a list.
    c = HeaderCarrier({})
    c["traceparent"] = TP
    assert [c.get("traceparent")] == [TP]
