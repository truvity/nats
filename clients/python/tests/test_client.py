"""The driver glue that needs no broker: per-handshake credential reads and the guarded hooks."""

from __future__ import annotations

import asyncio
import shutil
import ssl
import subprocess
from pathlib import Path
from typing import Any

import pytest
from nats import errors as nats_errors

from truvity_nats import CredentialError, NatsConfig
from truvity_nats.client import _build_ssl_context, _Connection, _read_token


def make_ca(path: Path, cn: str) -> None:
    key = path.with_suffix(".key")
    subprocess.run(
        ["openssl", "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", str(key)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            *(["openssl", "req", "-x509", "-new", "-key"]),
            str(key),
            "-sha256",
            "-days",
            "2",
            "-subj",
            f"/CN={cn}",
            "-out",
            str(path),
        ],
        check=True,
        capture_output=True,
    )


def test_token_is_read_from_the_file_every_time(tmp_path: Path) -> None:
    f = tmp_path / "token"
    f.write_text("one\n")
    assert _read_token(str(f)) == "one"
    f.write_text("  two  \n")
    assert _read_token(str(f)) == "two"
    f.unlink()
    with pytest.raises(CredentialError, match="unreadable"):
        _read_token(str(f))


def test_ssl_context_without_a_ca_is_refused() -> None:
    with pytest.raises(nats_errors.SecureConnRequiredError):
        _build_ssl_context(NatsConfig(url="nats://h:4222", token_file="/t"))


def test_ssl_context_with_an_unusable_ca_is_a_credential_error(tmp_path: Path) -> None:
    with pytest.raises(CredentialError, match="CA file"):
        _build_ssl_context(NatsConfig(url="tls://h", ca_file=str(tmp_path / "missing"), token_file="/t"))
    junk = tmp_path / "junk.crt"
    junk.write_text("not a certificate")
    with pytest.raises(CredentialError, match="CA file"):
        _build_ssl_context(NatsConfig(url="tls://h", ca_file=str(junk), token_file="/t"))


@pytest.mark.skipif(shutil.which("openssl") is None, reason="needs the openssl CLI")
def test_ssl_context_verifies_and_trusts_only_the_ca_file_read_now(tmp_path: Path) -> None:
    make_ca(tmp_path / "a.crt", "ca-a")
    make_ca(tmp_path / "b.crt", "ca-b")
    ca = tmp_path / "ca.crt"
    shutil.copyfile(tmp_path / "a.crt", ca)
    cfg = NatsConfig(url="tls://h", ca_file=str(ca), token_file="/t")

    ctx = _build_ssl_context(cfg)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname
    assert ctx.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert [c["subject"][0][0][1] for c in ctx.get_ca_certs()] == ["ca-a"]  # not the system store

    shutil.copyfile(tmp_path / "b.crt", ca)  # a renewed CA, no restart
    assert [c["subject"][0][0][1] for c in _build_ssl_context(cfg).get_ca_certs()] == ["ca-b"]


@pytest.mark.skipif(shutil.which("openssl") is None, reason="needs the openssl CLI")
def test_client_certificate_that_cannot_be_loaded_is_a_credential_error(tmp_path: Path) -> None:
    make_ca(tmp_path / "ca.crt", "ca")
    cfg = NatsConfig(
        url="tls://h",
        ca_file=str(tmp_path / "ca.crt"),
        cert_file=str(tmp_path / "none.crt"),
        key_file=str(tmp_path / "none.key"),
    )
    with pytest.raises(CredentialError, match="client certificate"):
        _build_ssl_context(cfg)


def test_a_broker_without_tls_is_refused_when_a_ca_is_given() -> None:
    conn = _Connection(NatsConfig(url="tls://h", ca_file="/ca", token_file="/t"))
    with pytest.raises(nats_errors.SecureConnWantedError):
        asyncio.run(conn._process_info({"server_id": "x"}, initial_connection=True))


@pytest.mark.parametrize(
    "msg", ["'Authorization Violation'", "'User Authentication Expired'", "'User Authentication Revoked'"]
)
def test_authorization_errors_on_an_open_connection_reconnect_instead_of_closing(msg: str) -> None:
    conn = _Connection(NatsConfig(url="nats://h", token_file="/t"))
    seen: list[Exception] = []

    async def fake_op_err(e: Exception) -> None:
        seen.append(e)

    async def fail_close(*_a: Any, **_k: Any) -> None:  # pragma: no cover
        raise AssertionError("the connection must not be closed")

    conn._process_op_err = fake_op_err  # type: ignore[method-assign]
    conn._close = fail_close  # type: ignore[method-assign]
    asyncio.run(conn._process_err(msg))
    assert len(seen) == 1
