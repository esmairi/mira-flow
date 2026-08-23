"""Centralized transport settings — env vars (or a local .env), validated once.

A leaf module: no imports from wiring/ledger/orchestrator/core, so any of them can
depend on it without violating the §7 "packages don't import each other"
rule. Scoped to transport concerns only — `database_url` is a `ledger`
concern and lives in `ledger.db`'s own settings instead (§7): nothing here
should need to know the ledger exists.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Kafka connection + auth config.

    Auth is deliberately generic rather than one hardcoded mechanism:
    whatever `security.protocol`/`sasl.*`/`ssl.*` values are set here are
    forwarded straight into confluent-kafka's client config (see
    `kafka_auth_params()`), so SASL_PLAIN, SASL_SCRAM, mTLS, etc. all work
    through the same fields — the client library is already the generic
    layer, this just wires values into it from two Kubernetes-friendly
    sources instead of the workflow JSON config, which must never carry
    secrets:

    1. Environment variables (e.g. `KAFKA_SASL_PASSWORD`) — the usual route
       for a Kubernetes Secret injected via `env.valueFrom.secretKeyRef`.
    2. One-file-per-value secrets, read from `KAFKA_SECRETS_DIR` if set
       (e.g. `KAFKA_SECRETS_DIR=/etc/secrets/kafka`, with a file named
       `kafka_sasl_password` in it) — the route for a Secret mounted as a
       volume, which never appears in `env`/`kubectl describe pod`/process
       listings the way an env var can. Unset by default: nothing extra
       happens (no warning, no filesystem access) unless a deployment opts
       in by setting it.

    A value set both ways always resolves to the environment variable —
    file-based secrets are the fallback, not an override.

    Attributes:
        kafka_bootstrap_servers: Kafka bootstrap servers.
        kafka_security_protocol: e.g. "SASL_SSL", "SSL", "SASL_PLAINTEXT".
            None (PLAINTEXT, confluent-kafka's default) if unset.
        kafka_sasl_mechanism: e.g. "PLAIN", "SCRAM-SHA-512", "OAUTHBEARER".
        kafka_sasl_username: SASL username.
        kafka_sasl_password: SASL password.
        kafka_ssl_ca_location: Path to the CA certificate, for verifying
            the broker (SSL/SASL_SSL).
        kafka_ssl_certificate_location: Path to the client certificate,
            for mTLS.
        kafka_ssl_key_location: Path to the client private key, for mTLS.
        kafka_ssl_key_password: Password for `kafka_ssl_key_location`, if
            the key is encrypted.
        kafka_publish_timeout_seconds: How long `publish()` blocks waiting
            for delivery confirmation (`Producer.flush()`) before raising.
        kafka_close_timeout_seconds: How long `close()` blocks flushing any
            outstanding messages on shutdown.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str | None = None
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    kafka_ssl_ca_location: str | None = None
    kafka_ssl_certificate_location: str | None = None
    kafka_ssl_key_location: str | None = None
    kafka_ssl_key_password: SecretStr | None = None
    kafka_publish_timeout_seconds: float = 30.0
    kafka_close_timeout_seconds: float = 10.0

    def kafka_auth_params(self) -> dict[str, Any]:
        """Builds the confluent-kafka client config for whatever auth is set.

        Merge this in as the lowest-priority layer of a consumer/producer
        config — a topic's own `params` (§2) can still override any of
        these per topic if it ever needs to.

        Returns:
            A dict of confluent-kafka `security.protocol`/`sasl.*`/`ssl.*`
            keys — only the ones actually configured; empty if none are.
        """
        pairs = (
            ("security.protocol", self.kafka_security_protocol),
            ("sasl.mechanism", self.kafka_sasl_mechanism),
            ("sasl.username", self.kafka_sasl_username),
            ("sasl.password", self.kafka_sasl_password),
            ("ssl.ca.location", self.kafka_ssl_ca_location),
            ("ssl.certificate.location", self.kafka_ssl_certificate_location),
            ("ssl.key.location", self.kafka_ssl_key_location),
            ("ssl.key.password", self.kafka_ssl_key_password),
        )
        return {
            key: value.get_secret_value() if isinstance(value, SecretStr) else value
            for key, value in pairs
            if value is not None
        }


settings = Settings(_secrets_dir=os.environ.get("KAFKA_SECRETS_DIR"))
