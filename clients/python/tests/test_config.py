from __future__ import annotations

import pytest

from truvity_nats import ConfigError, NatsConfig, parse_duration, redact_url


def cfg(**kw: object) -> NatsConfig:
    base: dict[str, object] = {"url": "nats://localhost:4222", "token_file": "/token", "name": "t"}
    base.update(kw)
    return NatsConfig(**base)  # type: ignore[arg-type]


def test_valid_token_and_certificate_configs() -> None:
    cfg().validate()
    cfg(url="tls://localhost:4222", ca_file="/ca", token_file="", cert_file="/c", key_file="/k").validate()
    cfg(url="nats://[::1]:4222").validate()


def test_defaults_match_the_contract() -> None:
    c = NatsConfig()
    assert (c.connect_timeout, c.reconnect_wait, c.ping_interval, c.drain_timeout) == (5.0, 1.0, 30.0, 10.0)
    assert (c.retry.attempts, c.retry.max_delay, c.retry.budget) == (5, 5.0, 30.0)


def test_validate_lists_every_problem() -> None:
    with pytest.raises(ConfigError) as ei:
        NatsConfig(url="tls://localhost", cert_file="/c", token_file="/t", connect_timeout=0).validate()
    text = str(ei.value)
    for want in ("server CA", "go together", "two identities", "must be positive"):
        assert want in text
    assert len(ei.value.problems) >= 4


@pytest.mark.parametrize(
    ("url", "want"),
    [
        ("", "URL is required"),
        ("http://localhost:4222", "scheme"),
        ("localhost:4222", "not a nats:// or tls://"),
        ("nats://", "not a nats:// or tls://"),
        ("nats://a:4222,nats://b:4222", "exactly one URL"),
        ("nats://user:pw@localhost:4222", "credentials in the URL"),
        ("nats://localhost:notaport", "not a nats:// or tls://"),
    ],
)
def test_bad_urls(url: str, want: str) -> None:
    with pytest.raises(ConfigError, match=want) as ei:
        cfg(url=url).validate()
    assert "pw" not in str(ei.value)


def test_certificate_needs_a_ca_and_a_credential_is_required() -> None:
    with pytest.raises(ConfigError, match="needs the server CA"):
        cfg(token_file="", cert_file="/c", key_file="/k").validate()
    with pytest.raises(ConfigError, match="a credential is required"):
        cfg(token_file="").validate()


def test_repr_carries_no_secret() -> None:
    c = cfg(url="nats://user:pw@localhost:4222")
    for text in (repr(c), str(c), f"{c}"):
        assert "pw" not in text
        assert "user" not in text
    assert redact_url("nats://user:pw@h:1") == "nats://h:1"
    assert redact_url("nats://h:1") == "nats://h:1"


def test_from_env() -> None:
    c = NatsConfig.from_env(
        {
            "NATS_URL": "tls://localhost:4222",
            "NATS_CA_FILE": "/ca",
            "NATS_CERT_FILE": "/c",
            "NATS_KEY_FILE": "/k",
            "NATS_CLIENT_NAME": "svc",
            "NATS_CLIENT_CONNECT_TIMEOUT": "500ms",
            "NATS_CLIENT_RECONNECT_WAIT": "2s",
            "NATS_CLIENT_PING_INTERVAL": "1m30s",
            "NATS_CLIENT_DRAIN_TIMEOUT": "1.5s",
            "NATS_CLIENT_RETRY_ATTEMPTS": "7",
            "NATS_CLIENT_RETRY_MAX_DELAY": "2s",
            "NATS_CLIENT_RETRY_BUDGET": "1m",
        }
    )
    assert (c.url, c.ca_file, c.cert_file, c.key_file, c.name) == (
        "tls://localhost:4222",
        "/ca",
        "/c",
        "/k",
        "svc",
    )
    assert (c.connect_timeout, c.reconnect_wait, c.ping_interval, c.drain_timeout) == (0.5, 2.0, 90.0, 1.5)
    assert (c.retry.attempts, c.retry.max_delay, c.retry.budget) == (7, 2.0, 60.0)


def test_from_env_reports_bad_values_and_defaults_the_name() -> None:
    with pytest.raises(ConfigError) as ei:
        NatsConfig.from_env(
            {
                "NATS_URL": "nats://h",
                "NATS_TOKEN_FILE": "/t",
                "NATS_CLIENT_PING_INTERVAL": "soon",
                "NATS_CLIENT_RETRY_ATTEMPTS": "x",
            }
        )
    assert "NATS_CLIENT_PING_INTERVAL" in str(ei.value)
    assert "NATS_CLIENT_RETRY_ATTEMPTS" in str(ei.value)
    ok = NatsConfig.from_env({"NATS_URL": "nats://h", "NATS_TOKEN_FILE": "/t"})
    assert ok.name  # the program name


def test_from_env_validates() -> None:
    with pytest.raises(ConfigError, match="URL is required"):
        NatsConfig.from_env({})


@pytest.mark.parametrize(
    ("text", "want"),
    [("5s", 5.0), ("500ms", 0.5), ("1m30s", 90.0), ("1.5h", 5400.0), ("250us", 0.00025), ("0", 0.0)],
)
def test_parse_duration(text: str, want: float) -> None:
    assert parse_duration(text) == pytest.approx(want)


@pytest.mark.parametrize("text", ["", "5", "s", "-1s", "1d", "1s5", "abc"])
def test_parse_duration_rejects(text: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(text)
