"""Kafka Java client defaults that the lint rules depend on, by client version.

Only defaults that are documented in the Apache Kafka configuration reference
or the KIP that changed them are listed. A key whose default changed between
releases is resolved only when the client version is known.
"""

from __future__ import annotations

# key -> list of (first version, default) in ascending order.
VERSIONED_DEFAULTS: dict[str, list[tuple[tuple[int, int], str]]] = {
    # KIP-679 (Kafka 3.0): strongest delivery guarantees by default.
    "acks": [((0, 0), "1"), ((3, 0), "all")],
    "enable.idempotence": [((0, 0), "false"), ((3, 0), "true")],
    # KIP-1030 (Kafka 4.0): linger.ms default 0 -> 5.
    "linger.ms": [((0, 0), "0"), ((4, 0), "5")],
    # KIP-735 (Kafka 3.0): session.timeout.ms default 10s -> 45s.
    "session.timeout.ms": [((0, 0), "10000"), ((3, 0), "45000")],
    # KIP-91 (Kafka 2.1): retries default 0 -> MAX_INT, bounded by delivery.timeout.ms.
    "retries": [((0, 0), "0"), ((2, 1), "2147483647")],
}

STABLE_DEFAULTS: dict[str, str] = {
    "max.in.flight.requests.per.connection": "5",
    "batch.size": "16384",
    "compression.type": "none",
    "request.timeout.ms": "30000",
    "delivery.timeout.ms": "120000",  # added in 2.1
    "enable.auto.commit": "true",
    "auto.commit.interval.ms": "5000",
    "max.poll.interval.ms": "300000",
    "max.poll.records": "500",
    "heartbeat.interval.ms": "3000",
    "isolation.level": "read_uncommitted",
    "auto.offset.reset": "latest",
    "fetch.min.bytes": "1",
    "fetch.max.wait.ms": "500",
    "group.protocol": "classic",
}

COMMON_KEYS = {
    "bootstrap.servers", "client.id", "client.dns.lookup", "client.rack",
    "security.protocol", "sasl.mechanism", "sasl.jaas.config",
    "sasl.login.callback.handler.class", "sasl.client.callback.handler.class",
    "sasl.oauthbearer.token.endpoint.url",
    "ssl.truststore.location", "ssl.truststore.password", "ssl.truststore.type",
    "ssl.keystore.location", "ssl.keystore.password", "ssl.keystore.type",
    "ssl.key.password", "ssl.endpoint.identification.algorithm", "ssl.protocol",
    "request.timeout.ms", "retry.backoff.ms", "retry.backoff.max.ms",
    "reconnect.backoff.ms", "reconnect.backoff.max.ms", "metadata.max.age.ms",
    "connections.max.idle.ms", "send.buffer.bytes", "receive.buffer.bytes",
    "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms",
    "metric.reporters", "metrics.num.samples", "metrics.sample.window.ms",
    "metrics.recording.level", "interceptor.classes", "default.api.timeout.ms",
    "enable.metrics.push",
}

PRODUCER_KEYS = COMMON_KEYS | {
    "key.serializer", "value.serializer", "acks", "enable.idempotence", "retries",
    "max.in.flight.requests.per.connection", "linger.ms", "batch.size",
    "compression.type", "compression.gzip.level", "compression.lz4.level",
    "compression.zstd.level", "buffer.memory", "max.block.ms",
    "max.request.size", "delivery.timeout.ms", "partitioner.class",
    "partitioner.ignore.keys", "partitioner.adaptive.partitioning.enable",
    "partitioner.availability.timeout.ms", "transactional.id",
    "transaction.timeout.ms", "metadata.max.idle.ms",
}

CONSUMER_KEYS = COMMON_KEYS | {
    "key.deserializer", "value.deserializer", "group.id", "group.instance.id",
    "group.protocol", "group.remote.assignor", "enable.auto.commit",
    "auto.commit.interval.ms", "auto.offset.reset", "isolation.level",
    "max.poll.records", "max.poll.interval.ms", "session.timeout.ms",
    "heartbeat.interval.ms", "fetch.min.bytes", "fetch.max.bytes",
    "fetch.max.wait.ms", "max.partition.fetch.bytes",
    "partition.assignment.strategy", "check.crcs", "exclude.internal.topics",
    "allow.auto.create.topics", "client.rack",
}

INT_KEYS = {
    "retries", "max.in.flight.requests.per.connection", "linger.ms", "batch.size",
    "request.timeout.ms", "delivery.timeout.ms", "auto.commit.interval.ms",
    "max.poll.interval.ms", "max.poll.records", "session.timeout.ms",
    "heartbeat.interval.ms", "fetch.min.bytes", "fetch.max.wait.ms",
    "buffer.memory", "max.request.size", "fetch.max.bytes",
}
BOOL_KEYS = {"enable.idempotence", "enable.auto.commit"}
ENUM_KEYS = {
    "acks": {"all", "-1", "0", "1"},
    "compression.type": {"none", "gzip", "snappy", "lz4", "zstd"},
    "isolation.level": {"read_committed", "read_uncommitted"},
    "group.protocol": {"classic", "consumer"},
}


def parse_version(text: str) -> tuple[int, int]:
    parts = text.strip().split(".")
    try:
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except ValueError as exc:
        raise ValueError(f"invalid Kafka version {text!r}; use e.g. 3.7") from exc


def default_for(key: str, version: tuple[int, int] | None) -> str | None:
    """The client default for `key`, or None if unknown for this version."""
    if key in STABLE_DEFAULTS:
        return STABLE_DEFAULTS[key]
    history = VERSIONED_DEFAULTS.get(key)
    if history is None or version is None:
        return None
    value = None
    for since, default in history:
        if version >= since:
            value = default
    return value


def default_history(key: str) -> str:
    """Human-readable default history, e.g. '1 before 3.0, all from 3.0'."""
    history = VERSIONED_DEFAULTS.get(key, [])
    parts = []
    for i, (since, value) in enumerate(history):
        if i + 1 < len(history):
            nxt = history[i + 1][0]
            parts.append(f"{value} before {nxt[0]}.{nxt[1]}")
        else:
            parts.append(f"{value} from {since[0]}.{since[1]}")
    return ", ".join(parts)
