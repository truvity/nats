"""The broker's namespace-to-account rule and the publish-subject rule (shared vectors)."""

from __future__ import annotations

from collections.abc import Collection

MAX_SUBJECT_LENGTH = 255

_TOKEN_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")


class NamespaceError(ValueError):
    """A namespace that has no NATS account."""


class SubjectError(ValueError):
    """A subject a client may not publish to."""


def account_for_namespace(namespace: str, project_accounts: Collection[str] = ()) -> str:
    """
    The account a client of ``namespace`` lands in. A namespace in
    ``project_accounts``, ``emp-<slug>``, ``ci-<org>-<repo>`` and exactly ``ci``
    each map to the account of the same name; anything else has none
    (:class:`NamespaceError`). Matching is exact and case-sensitive. The callout
    applies the same rule; the shared vectors keep every language agreeing.

    Subjects are not prefixed with the account: the account boundary is the isolation.
    """
    if namespace in project_accounts:
        return namespace
    if namespace.startswith("emp-") and len(namespace) > len("emp-"):
        return namespace
    if namespace.startswith("ci-"):
        org, sep, repo = namespace[len("ci-") :].partition("-")
        if sep and org and repo:
            return namespace
    if namespace == "ci":
        return namespace
    raise NamespaceError(f'natsclient: namespace "{namespace}" has no NATS account mapping')


def valid_publish_subject(subject: str) -> None:
    """
    Checks a subject a client publishes to: concrete (no wildcards), dot-separated
    non-empty tokens of ASCII letters, digits, ``-`` and ``_``, at most 255 bytes, and
    not in the broker's reserved space (``$`` prefix, ``_INBOX.``). Raises :class:`SubjectError`.
    """
    if not subject:
        raise SubjectError("natsclient: subject is empty")
    if len(subject.encode()) > MAX_SUBJECT_LENGTH:
        raise SubjectError(f"natsclient: subject is longer than {MAX_SUBJECT_LENGTH} bytes")
    if subject.startswith(("$", "_INBOX.")):
        raise SubjectError(f'natsclient: subject "{subject}" is in the broker\'s reserved space')
    for token in subject.split("."):
        if not token:
            raise SubjectError(f'natsclient: subject "{subject}" has an empty token')
        for ch in token:
            if ch not in _TOKEN_CHARS:
                raise SubjectError(
                    f'natsclient: subject "{subject}": character {ch!r} is not allowed '
                    "(no wildcards, spaces or other punctuation)"
                )


def is_valid_publish_subject(subject: str) -> bool:
    """The boolean form of :func:`valid_publish_subject`."""
    try:
        valid_publish_subject(subject)
    except SubjectError:
        return False
    return True
