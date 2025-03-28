"""Lint rules for Kafka producer and consumer configs."""

from __future__ import annotations

import difflib
from dataclasses import dataclass

from .defaults import (
    BOOL_KEYS,
    CONSUMER_KEYS,
    ENUM_KEYS,
    INT_KEYS,
    PRODUCER_KEYS,
    VERSIONED_DEFAULTS,
    default_for,
    default_history,
)
from .loader import ClientConfig

PROFILES = ("durability", "latency", "throughput")
LEVELS = ("info", "warning", "error")


@dataclass
class Finding:
    level: str
    rule: str
    key: str
    message: str
    why: str
    line: int | None = None

    def to_dict(self) -> dict:
        return {"level": self.level, "rule": self.rule, "key": self.key,
                "message": self.message, "why": self.why, "line": self.line}


class Ctx:
    """Resolves effective values: explicit setting, else the version's default."""

    def __init__(self, cfg: ClientConfig, version: tuple[int, int] | None):
        self.cfg = cfg
        self.version = version
        self.findings: list[Finding] = []

    def explicit(self, key: str) -> bool:
        return key in self.cfg.values

    def value(self, key: str) -> str | None:
        raw = self.cfg.get(key)
        if raw is not None:
            return raw.strip().lower()
        return default_for(key, self.version)

    def int(self, key: str) -> int | None:
        v = self.value(key)
        try:
            return int(v) if v is not None else None
        except ValueError:
            return None

    def shown(self, key: str) -> str:
        v = self.value(key)
        if self.explicit(key):
            return f"{key}={self.cfg.get(key)}"
        return f"{key} unset (default {v})" if v is not None else f"{key} unset"

    def add(self, level, rule, key, message, why):
        self.findings.append(Finding(level, rule, key, message, why, self.cfg.lines.get(key)))

    def unknown_default(self, key: str, rule: str, level: str, advice: str) -> bool:
        """Report a version-dependent default that cannot be resolved."""
        if self.value(key) is None and key in VERSIONED_DEFAULTS:
            self.add(level, rule, key, f"{key} is unset and its default depends on the client version",
                     f"Default is {default_history(key)}. {advice} "
                     f"(or pass --kafka-version).")
            return True
        return False


# --- shared checks -----------------------------------------------------------

def check_values(ctx: Ctx) -> None:
    for key, raw in ctx.cfg.values.items():
        v = raw.strip().lower()
        if key in INT_KEYS:
            try:
                int(v)
            except ValueError:
                ctx.add("error", "invalid-value", key, f"{key}={raw} is not an integer",
                        "The Java client rejects non-numeric values for this key at startup.")
        elif key in BOOL_KEYS and v not in ("true", "false"):
            ctx.add("error", "invalid-value", key, f"{key}={raw} is not true/false",
                    "The Java client only accepts true or false here.")
        elif key in ENUM_KEYS and v not in ENUM_KEYS[key]:
            ctx.add("error", "invalid-value", key, f"{key}={raw} is not a valid value",
                    f"Valid values: {', '.join(sorted(ENUM_KEYS[key]))}.")


def check_unknown_keys(ctx: Ctx) -> None:
    known = PRODUCER_KEYS if ctx.cfg.client_type == "producer" else CONSUMER_KEYS
    other = CONSUMER_KEYS if ctx.cfg.client_type == "producer" else PRODUCER_KEYS
    for key in ctx.cfg.values:
        if key in known:
            continue
        if key in other:
            ctx.add("warning", "wrong-client", key,
                    f"{key} is not a {ctx.cfg.client_type} setting",
                    "The client logs it as an unused config and ignores it; "
                    "it may belong in the other client's file.")
            continue
        close = difflib.get_close_matches(key, sorted(known), n=1, cutoff=0.85)
        if close:
            ctx.add("warning", "unknown-key", key, f"{key} looks like a typo of {close[0]}",
                    "Unknown keys are ignored by the client, so the intended setting "
                    "silently keeps its default.")


# --- producer ----------------------------------------------------------------

def _idempotence_conflicts(ctx: Ctx) -> list[str]:
    conflicts = []
    acks = ctx.value("acks")
    if acks is not None and acks not in ("all", "-1"):
        conflicts.append(f"acks={acks}")
    retries = ctx.int("retries")
    if retries == 0:
        conflicts.append("retries=0")
    inflight = ctx.int("max.in.flight.requests.per.connection")
    if inflight is not None and inflight > 5:
        conflicts.append(f"max.in.flight.requests.per.connection={inflight}")
    return conflicts


def check_producer(ctx: Ctx, profile: str) -> None:
    acks = ctx.value("acks")
    idem_explicit = ctx.explicit("enable.idempotence")
    idem = ctx.value("enable.idempotence")
    conflicts = _idempotence_conflicts(ctx)
    silently_disabled = False

    if idem_explicit and idem == "true" and conflicts:
        ctx.add("error", "idempotence-conflict", "enable.idempotence",
                f"enable.idempotence=true conflicts with {', '.join(conflicts)}",
                "Idempotence requires acks=all, retries > 0 and "
                "max.in.flight.requests.per.connection <= 5. When it is set "
                "explicitly with conflicting values the producer fails with a ConfigException.")
    elif not idem_explicit and idem == "true" and conflicts:
        ctx.add("warning", "idempotence-disabled", "enable.idempotence",
                f"idempotence is silently disabled by {', '.join(conflicts)}",
                "Idempotence is on by default only when no conflicting settings are "
                "present; otherwise it is turned off without an error, so retries can "
                "write duplicates.")
        idem = "false"
        silently_disabled = True

    if profile == "durability":
        if not ctx.unknown_default("acks", "acks", "warning", "Set acks=all explicitly"):
            if acks in ("0", "1"):
                waits = "only the partition leader" if acks == "1" else "no broker at all"
                ctx.add("error", "acks", "acks", ctx.shown("acks"),
                        f"With acks={acks} a send is acknowledged after {waits} has "
                        "it; a leader failure before replication loses acknowledged "
                        "records. Use acks=all.")
        if idem == "false" and not silently_disabled:
            ctx.add("error" if idem_explicit else "warning", "idempotence",
                    "enable.idempotence", ctx.shown("enable.idempotence"),
                    "Without idempotence a retried send can be written twice. "
                    "Set enable.idempotence=true.")
        elif idem is None:
            ctx.unknown_default("enable.idempotence", "idempotence", "warning",
                                "Set enable.idempotence=true explicitly")
        if ctx.int("retries") == 0:
            ctx.add("error", "retries", "retries", "retries=0",
                    "Transient errors (leader election, timeouts) fail the send "
                    "immediately. Leave retries at its default and bound retry time "
                    "with delivery.timeout.ms instead.")
        if acks in ("all", "-1"):
            ctx.add("info", "min-insync", "acks",
                    "acks=all is only as strong as the topic's min.insync.replicas",
                    "acks=all waits for the current in-sync replicas, which can shrink "
                    "to the leader alone. Check that topics use replication.factor >= 3 "
                    "and min.insync.replicas >= 2 (the broker default is 1); this cannot "
                    "be seen from the client config.")

    inflight = ctx.int("max.in.flight.requests.per.connection")
    retries = ctx.int("retries")
    if idem == "false" and inflight is not None and inflight > 1 and retries != 0:
        ctx.add("warning", "ordering", "max.in.flight.requests.per.connection",
                f"max.in.flight.requests.per.connection={inflight} without idempotence",
                "With retries and more than one in-flight request, a failed batch can "
                "be retried after a later one succeeded, reordering records. Enable "
                "idempotence or set it to 1.")

    delivery = ctx.int("delivery.timeout.ms")
    request = ctx.int("request.timeout.ms")
    linger = ctx.int("linger.ms")
    if delivery is not None and request is not None and delivery < request + (linger or 0):
        if ctx.explicit("delivery.timeout.ms"):
            ctx.add("error", "delivery-timeout", "delivery.timeout.ms",
                    f"delivery.timeout.ms={delivery} < request.timeout.ms + linger.ms "
                    f"({request + (linger or 0)})",
                    "delivery.timeout.ms must be at least linger.ms + request.timeout.ms; "
                    "the producer refuses to start when it is set lower explicitly.")
        else:
            ctx.add("warning", "delivery-timeout", "delivery.timeout.ms",
                    f"default delivery.timeout.ms={delivery} < request.timeout.ms + "
                    f"linger.ms ({request + (linger or 0)})",
                    "The producer raises delivery.timeout.ms to linger.ms + "
                    "request.timeout.ms and logs a warning; set it explicitly.")

    compression = ctx.value("compression.type")
    batch = ctx.int("batch.size")
    if profile == "latency":
        if linger is not None and linger > 10:
            ctx.add("warning", "linger", "linger.ms", ctx.shown("linger.ms"),
                    "Each record can wait up to linger.ms before being sent. Use 0-5 ms "
                    "for latency-sensitive producers.")
        if compression == "gzip":
            ctx.add("info", "compression", "compression.type", "compression.type=gzip",
                    "gzip costs the most CPU per batch; lz4 or zstd usually add less latency.")
    if profile == "throughput":
        if not ctx.unknown_default("linger.ms", "linger", "info", "Set linger.ms (e.g. 10-50)"):
            if linger is not None and linger < 5:
                ctx.add("warning", "linger", "linger.ms", ctx.shown("linger.ms"),
                        "A short linger sends small, frequent batches. 10-50 ms usually "
                        "gives bigger batches and better compression.")
        if batch is not None and batch < 65536:
            ctx.add("info", "batch-size", "batch.size", ctx.shown("batch.size"),
                    "Larger batches (64-256 KiB) cut request overhead under load; "
                    "memory comes from buffer.memory.")
        if compression in (None, "none"):
            ctx.add("warning", "compression", "compression.type", ctx.shown("compression.type"),
                    "Uncompressed batches use more network and disk. lz4 or zstd are "
                    "common choices.")


# --- consumer ----------------------------------------------------------------

def check_consumer(ctx: Ctx, profile: str, processing_ms: float | None) -> None:
    protocol = ctx.value("group.protocol")
    auto_commit = ctx.value("enable.auto.commit")
    if ctx.cfg.spring and not ctx.explicit("enable.auto.commit"):
        auto_commit = "false"  # Spring Kafka containers default it to false

    if profile == "durability":
        if auto_commit == "true":
            ctx.add("warning", "auto-commit", "enable.auto.commit", ctx.shown("enable.auto.commit"),
                    "Offsets are committed on a timer inside poll(). If records are "
                    "processed asynchronously, a crash can skip records whose offsets "
                    "were committed before processing finished. Commit after processing.")
        if ctx.value("auto.offset.reset") == "latest":
            ctx.add("info", "offset-reset", "auto.offset.reset", ctx.shown("auto.offset.reset"),
                    "A new group, or one whose committed offsets expired, starts at the "
                    "end of the log and skips existing records. Use earliest if every "
                    "record must be processed.")
        if ctx.value("isolation.level") == "read_uncommitted":
            ctx.add("info", "isolation", "isolation.level", ctx.shown("isolation.level"),
                    "If producers write transactionally, read_uncommitted also returns "
                    "records from aborted transactions. Use read_committed for "
                    "transactional pipelines.")

    if protocol == "consumer":
        for key in ("session.timeout.ms", "heartbeat.interval.ms"):
            if ctx.explicit(key):
                ctx.add("warning", "group-protocol", key, f"{key} with group.protocol=consumer",
                        f"With the consumer group protocol this is controlled by the "
                        f"broker's group.consumer.{key}, not the client.")
    else:
        session = ctx.int("session.timeout.ms")
        heartbeat = ctx.int("heartbeat.interval.ms")
        if session is None:
            if ctx.explicit("heartbeat.interval.ms"):
                ctx.unknown_default("session.timeout.ms", "heartbeat", "info",
                                    "Set session.timeout.ms explicitly")
        elif heartbeat is not None:
            if heartbeat >= session:
                ctx.add("error", "heartbeat", "heartbeat.interval.ms",
                        f"heartbeat.interval.ms={heartbeat} >= session.timeout.ms={session}",
                        "The heartbeat interval must be lower than the session timeout.")
            elif heartbeat * 3 > session:
                ctx.add("warning", "heartbeat", "heartbeat.interval.ms",
                        f"heartbeat.interval.ms={heartbeat} is more than a third of "
                        f"session.timeout.ms={session}",
                        "Kafka recommends a heartbeat no higher than 1/3 of the session "
                        "timeout, so one lost heartbeat does not evict the member.")

    max_poll_interval = ctx.int("max.poll.interval.ms")
    max_records = ctx.int("max.poll.records")
    if processing_ms is not None and max_poll_interval and max_records:
        worst = max_records * processing_ms
        if worst > max_poll_interval:
            ctx.add("error", "poll-interval", "max.poll.interval.ms",
                    f"{max_records} records x {processing_ms:g} ms = {worst:,.0f} ms > "
                    f"max.poll.interval.ms={max_poll_interval}",
                    "If a full batch takes longer than max.poll.interval.ms the consumer "
                    "is removed from the group and its partitions are rebalanced. Lower "
                    "max.poll.records or raise max.poll.interval.ms.")
        elif worst > 0.8 * max_poll_interval:
            ctx.add("warning", "poll-interval", "max.poll.interval.ms",
                    f"{max_records} records x {processing_ms:g} ms = {worst:,.0f} ms is "
                    f"over 80% of max.poll.interval.ms={max_poll_interval}",
                    "A slow batch would push the consumer out of the group. Leave headroom.")

    fetch_min = ctx.int("fetch.min.bytes")
    fetch_wait = ctx.int("fetch.max.wait.ms")
    if profile == "latency" and fetch_min is not None and fetch_min > 1 and fetch_wait and fetch_wait > 100:
        ctx.add("warning", "fetch-wait", "fetch.max.wait.ms",
                f"fetch.min.bytes={fetch_min} with fetch.max.wait.ms={fetch_wait}",
                "On a quiet topic the broker holds each fetch up to fetch.max.wait.ms "
                "waiting for fetch.min.bytes. Lower one of them for low latency.")
    if profile == "throughput" and fetch_min == 1:
        ctx.add("info", "fetch-min", "fetch.min.bytes", ctx.shown("fetch.min.bytes"),
                "Fetches return as soon as any data exists. A larger fetch.min.bytes "
                "(e.g. 65536) gives fewer, bigger fetches at the cost of latency.")


def lint(cfg: ClientConfig, profile: str = "durability",
         version: tuple[int, int] | None = None,
         processing_ms: float | None = None) -> list[Finding]:
    ctx = Ctx(cfg, version)
    check_values(ctx)
    check_unknown_keys(ctx)
    if cfg.client_type == "producer":
        check_producer(ctx, profile)
    else:
        check_consumer(ctx, profile, processing_ms)
    for key in cfg.not_interpreted:
        ctx.add("info", "not-interpreted", key, f"{key} was not interpreted",
                "Only plain-valued Spring Boot keys and *.properties.* pass-through "
                "keys are mapped; check this one by hand.")
    rank = {lvl: i for i, lvl in enumerate(reversed(LEVELS))}
    ctx.findings.sort(key=lambda f: rank[f.level])
    return ctx.findings
