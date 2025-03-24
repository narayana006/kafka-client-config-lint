"""Load Kafka client configs from Java .properties and Spring Boot files."""

from __future__ import annotations

from dataclasses import dataclass, field

from .defaults import CONSUMER_KEYS, PRODUCER_KEYS


class LoadError(ValueError):
    pass


@dataclass
class ClientConfig:
    """Kafka client settings plus where they came from."""

    source: str
    values: dict[str, str]
    lines: dict[str, int] = field(default_factory=dict)
    client_type: str | None = None  # "producer" or "consumer"
    spring: bool = False
    not_interpreted: list[str] = field(default_factory=list)

    def get(self, key: str) -> str | None:
        return self.values.get(key)


def _unescape(value: str) -> str:
    out, i = [], 0
    while i < len(value):
        ch = value[i]
        if ch == "\\" and i + 1 < len(value):
            nxt = value[i + 1]
            out.append({"t": "\t", "n": "\n", "r": "\r", "f": "\f"}.get(nxt, nxt))
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _split_key_value(line: str) -> tuple[str, str]:
    """Split a logical properties line on the first unescaped '=', ':' or space."""
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "\\":
            i += 2
            continue
        if ch in "=: \t\f":
            key = line[:i]
            rest = line[i:].lstrip(" \t\f")
            if ch in " \t\f" and rest[:1] in ("=", ":"):
                rest = rest[1:]
            elif ch in "=:":
                rest = rest[1:]
            return _unescape(key), _unescape(rest.lstrip(" \t\f"))
        i += 1
    return _unescape(line), ""


def parse_properties(text: str) -> tuple[dict[str, str], dict[str, int]]:
    """Parse Java .properties text (comments, continuations, '=' ':' or space)."""
    values: dict[str, str] = {}
    lines: dict[str, int] = {}
    physical = text.splitlines()
    i = 0
    while i < len(physical):
        start = i + 1
        line = physical[i].lstrip(" \t\f")
        i += 1
        if not line or line[0] in "#!":
            continue
        # A line ending in an odd number of backslashes continues on the next.
        while (len(line) - len(line.rstrip("\\"))) % 2 == 1 and i < len(physical):
            line = line[:-1] + physical[i].lstrip(" \t\f")
            i += 1
        key, value = _split_key_value(line)
        values[key] = value.strip()
        lines[key] = start
    return values, lines


# Spring Boot first-class properties that map 1:1 to a Kafka key with the same
# value format. Duration and DataSize properties (e.g. heartbeat-interval,
# fetch-min-size) are not converted and are reported as not interpreted.
SPRING_PRODUCER = {
    "acks": "acks",
    "retries": "retries",
    "batch-size": "batch.size",
    "compression-type": "compression.type",
    "client-id": "client.id",
    "key-serializer": "key.serializer",
    "value-serializer": "value.serializer",
}
SPRING_CONSUMER = {
    "enable-auto-commit": "enable.auto.commit",
    "auto-offset-reset": "auto.offset.reset",
    "group-id": "group.id",
    "isolation-level": "isolation.level",
    "max-poll-records": "max.poll.records",
    "client-id": "client.id",
    "key-deserializer": "key.deserializer",
    "value-deserializer": "value.deserializer",
}


def _from_spring(values: dict[str, str], lines: dict[str, int], source: str) -> list[ClientConfig]:
    common: dict[str, str] = {}
    sides: dict[str, dict[str, str]] = {"producer": {}, "consumer": {}}
    where: dict[str, dict[str, int]] = {"producer": {}, "consumer": {}, "common": {}}
    skipped: list[str] = []
    for key, value in values.items():
        if not key.startswith("spring.kafka."):
            continue
        rest = key[len("spring.kafka."):]
        if rest.startswith("properties."):
            common[rest[len("properties."):]] = value
            where["common"][rest[len("properties."):]] = lines[key]
            continue
        side, _, sub = rest.partition(".")
        if side not in sides:
            skipped.append(key)
            continue
        if sub.startswith("properties."):
            kafka_key = sub[len("properties."):]
        else:
            kafka_key = (SPRING_PRODUCER if side == "producer" else SPRING_CONSUMER).get(sub)
        if kafka_key is None:
            skipped.append(key)
            continue
        sides[side][kafka_key] = value
        where[side][kafka_key] = lines[key]
    configs = []
    for side, vals in sides.items():
        if not vals:
            continue
        merged = {**common, **vals}
        merged_lines = {**where["common"], **where[side]}
        configs.append(ClientConfig(
            f"{source} (spring.kafka.{side})", merged, merged_lines, side, True,
            [k for k in skipped if k.startswith(f"spring.kafka.{side}.")],
        ))
    if not configs and common:
        configs.append(ClientConfig(f"{source} (spring.kafka)", common, where["common"],
                                    None, True, skipped))
    return configs


def detect_type(values: dict[str, str]) -> str | None:
    producer_only = PRODUCER_KEYS - CONSUMER_KEYS
    consumer_only = CONSUMER_KEYS - PRODUCER_KEYS
    p = sum(1 for k in values if k in producer_only)
    c = sum(1 for k in values if k in consumer_only)
    if p > c:
        return "producer"
    if c > p:
        return "consumer"
    return None


def load(text: str, source: str, client_type: str | None = None) -> list[ClientConfig]:
    values, lines = parse_properties(text)
    if any(k.startswith("spring.kafka.") for k in values):
        configs = _from_spring(values, lines, source)
    else:
        configs = [ClientConfig(source, values, lines)]
    for cfg in configs:
        if client_type and not cfg.spring:
            cfg.client_type = client_type
        elif cfg.client_type is None:
            cfg.client_type = detect_type(cfg.values)
        if cfg.client_type is None:
            raise LoadError(
                f"{cfg.source}: cannot tell producer from consumer; pass --type"
            )
    return configs
