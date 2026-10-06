"""Trace headers. Write the lower-case name exactly, read case-insensitively.

NATS headers are HTTP-like, and some drivers canonicalise a key (``traceparent``
becomes ``Traceparent``) while others send it as given. A reader that looks a
key up exactly then misses what another language wrote.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping

HEADER_TRACEPARENT = "traceparent"
HEADER_TRACESTATE = "tracestate"


class HeaderCarrier(MutableMapping[str, str]):
    """
    A view of a header dict (nats-py's ``Msg.headers`` shape) with the contract's casing:
    lookups ignore case, a write replaces every spelling with one lower-case entry and an
    empty value removes the key. It is a mapping with ``get``/``keys``, so an OpenTelemetry
    propagator accepts it as a carrier without this package importing OpenTelemetry.
    """

    def __init__(self, headers: MutableMapping[str, str]) -> None:
        self._h = headers

    def _find(self, key: str) -> list[str]:
        lk = key.lower()
        return [k for k in self._h if k.lower() == lk]

    def __getitem__(self, key: str) -> str:
        for k in self._find(key):
            if self._h[k]:
                return self._h[k]
        raise KeyError(key)

    def __setitem__(self, key: str, value: str) -> None:
        for k in self._find(key):
            del self._h[k]
        if value:
            self._h[key.lower()] = value

    def __delitem__(self, key: str) -> None:
        found = self._find(key)
        if not found:
            raise KeyError(key)
        for k in found:
            del self._h[k]

    def __iter__(self) -> Iterator[str]:
        return iter(list(self._h))

    def __len__(self) -> int:
        return len(self._h)


def inject_trace(headers: MutableMapping[str, str], traceparent: str = "", tracestate: str = "") -> None:
    """Writes the trace headers, replacing any spelling already there. An empty value is not written."""
    carrier = HeaderCarrier(headers)
    carrier[HEADER_TRACEPARENT] = traceparent
    carrier[HEADER_TRACESTATE] = tracestate


def extract_trace(headers: Mapping[str, str] | None) -> tuple[str, str]:
    """Reads the trace headers, whatever case they were written in. Missing ones are ``""``."""
    if not headers:
        return "", ""
    wanted = {HEADER_TRACEPARENT: "", HEADER_TRACESTATE: ""}
    for k, v in headers.items():
        lk = k.lower()
        if lk in wanted and not wanted[lk] and v:
            wanted[lk] = v
    return wanted[HEADER_TRACEPARENT], wanted[HEADER_TRACESTATE]
