"""The shared vectors (clients/conformance/vectors), with no broker."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from truvity_nats import (
    NamespaceError,
    SubjectError,
    account_for_namespace,
    is_valid_publish_subject,
    valid_publish_subject,
)

VECTORS = Path(__file__).resolve().parents[2] / "conformance" / "vectors"


def load(name: str) -> Any:
    return json.loads((VECTORS / name).read_text())


def test_account_for_namespace_vectors() -> None:
    v = load("account-for-namespace.json")
    assert v["cases"]
    for case in v["cases"]:
        ns = case["namespace"]
        if case.get("error"):
            with pytest.raises(NamespaceError):
                account_for_namespace(ns, v["projectAccounts"])
        else:
            assert account_for_namespace(ns, v["projectAccounts"]) == case["account"], ns


def test_subject_vectors() -> None:
    v = load("subjects.json")
    assert v["valid"]
    for s in v["valid"]:
        valid_publish_subject(s)
        assert is_valid_publish_subject(s)
    for s in v["invalid"]:
        with pytest.raises(SubjectError):
            valid_publish_subject(s)
        assert not is_valid_publish_subject(s)
    valid_publish_subject("a" * (v["tooLong"] - 1))
    with pytest.raises(SubjectError, match="longer"):
        valid_publish_subject("a" * v["tooLong"])


def test_the_length_limit_counts_bytes() -> None:
    assert not is_valid_publish_subject("é" * 10)  # not ASCII anyway
