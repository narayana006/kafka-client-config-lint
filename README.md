# kafka-client-config-lint

Lint Kafka producer and consumer configs against a durability, latency or throughput profile, with an explanation for every finding.

**Status:** v0.1. Reads Java `.properties` client configs and the `spring.kafka.*` keys in a Spring Boot `application.properties`, detects producer vs consumer, resolves Java client defaults by version, and checks them against one of three profiles. Text or JSON output, CI exit codes. Works entirely offline.

## Install

```bash
pip install git+https://github.com/narayana006/kafka-client-config-lint
```

Python 3.10+, no runtime dependencies.

## Quick start

The files in `examples/` are illustrative configs written for this README (hosts are `example.com`).

```bash
kafka-client-config-lint examples/producer.properties --kafka-version 3.7
```

```text
examples/producer.properties: producer, profile durability
  ERROR   acks: acks=1 (line 5)
          With acks=1 a send is acknowledged after only the partition leader has it; a leader failure before replication loses acknowledged records. Use acks=all.
  ERROR   delivery-timeout: delivery.timeout.ms=20000 < request.timeout.ms + linger.ms (30000) (line 10)
          delivery.timeout.ms must be at least linger.ms + request.timeout.ms; the producer refuses to start when it is set lower explicitly.
  WARNING unknown-key: compresion.type looks like a typo of compression.type (line 9)
          Unknown keys are ignored by the client, so the intended setting silently keeps its default.
  WARNING idempotence-disabled: idempotence is silently disabled by acks=1
          Idempotence is on by default only when no conflicting settings are present; otherwise it is turned off without an error, so retries can write duplicates.
  WARNING ordering: max.in.flight.requests.per.connection=5 without idempotence (line 7)
          With retries and more than one in-flight request, a failed batch can be retried after a later one succeeded, reordering records. Enable idempotence or set it to 1.

2 error(s), 3 warning(s), 0 info
```

Exit code `1` (errors present). A consumer, with the worst-case processing time per record:

```bash
kafka-client-config-lint examples/consumer.properties --processing-ms 800
```

```text
examples/consumer.properties: consumer, profile durability
  ERROR   poll-interval: 500 records x 800 ms = 400,000 ms > max.poll.interval.ms=300000 (line 8)
          If a full batch takes longer than max.poll.interval.ms the consumer is removed from the group and its partitions are rebalanced. Lower max.poll.records or raise max.poll.interval.ms.
  WARNING auto-commit: enable.auto.commit=true (line 6)
          Offsets are committed on a timer inside poll(). If records are processed asynchronously, a crash can skip records whose offsets were committed before processing finished. Commit after processing.
  WARNING heartbeat: heartbeat.interval.ms=5000 is more than a third of session.timeout.ms=10000 (line 10)
          Kafka recommends a heartbeat no higher than 1/3 of the session timeout, so one lost heartbeat does not evict the member.
  INFO    offset-reset: auto.offset.reset unset (default latest)
          A new group, or one whose committed offsets expired, starts at the end of the log and skips existing records. Use earliest if every record must be processed.
  INFO    isolation: isolation.level unset (default read_uncommitted)
          If producers write transactionally, read_uncommitted also returns records from aborted transactions. Use read_committed for transactional pipelines.

1 error(s), 2 warning(s), 2 info
```

## Usage

```text
kafka-client-config-lint FILE [FILE ...]
    [--profile durability|latency|throughput]   default: durability
    [--type producer|consumer]                  default: detect from keys
    [--kafka-version X.Y]                       client library version
    [--processing-ms MS]                        worst-case time per record
    [--format text|json]
    [--fail-on info|warning|error|never]        default: error
```

Exit codes: `0` nothing at or above `--fail-on`; `1` findings; `2` unreadable file, bad version string, or a file whose type cannot be detected (pass `--type`).

### Client defaults and `--kafka-version`

When a key is unset, the tool uses the Java client default. A few defaults changed between releases, and the tool only asserts those when you pass `--kafka-version`:

| Key | Default |
|---|---|
| `acks` | `1` before 3.0, `all` from 3.0 (KIP-679) |
| `enable.idempotence` | `false` before 3.0, `true` from 3.0 (KIP-679) |
| `linger.ms` | `0` before 4.0, `5` from 4.0 (KIP-1030) |
| `session.timeout.ms` | `10000` before 3.0, `45000` from 3.0 (KIP-735) |
| `retries` | `0` before 2.1, `2147483647` from 2.1 (KIP-91) |

Without a version, an unset key from this table produces a finding that says the default depends on the version and asks you to set it explicitly. Other defaults (`batch.size=16384`, `max.in.flight.requests.per.connection=5`, `enable.auto.commit=true`, `max.poll.records=500`, `max.poll.interval.ms=300000`, `heartbeat.interval.ms=3000`, `isolation.level=read_uncommitted`, `auto.offset.reset=latest`, ...) are taken from the current Kafka configuration reference.

These are the Apache Kafka Java client defaults. librdkafka-based clients (confluent-kafka-python, Go, .NET) have different defaults and are not modelled.

### What each profile checks

Every profile: invalid values for known keys, likely typos of known keys, keys that belong to the other client type, explicit `enable.idempotence=true` with conflicting `acks`/`retries`/`max.in.flight`, idempotence silently disabled by those conflicts, reordering risk (`max.in.flight > 1` without idempotence), `delivery.timeout.ms < linger.ms + request.timeout.ms`, heartbeat vs session timeout (classic protocol), session/heartbeat set with `group.protocol=consumer`, and `--processing-ms` vs `max.poll.records` x `max.poll.interval.ms`.

- **durability**: producer `acks` must be `all`, idempotence on, `retries` not 0, plus a reminder that `acks=all` depends on the topic's `min.insync.replicas` (not visible to a client). Consumer: `enable.auto.commit=true`, `auto.offset.reset=latest`, `isolation.level=read_uncommitted`.
- **latency**: producer `linger.ms` above 10 ms, `gzip` compression; consumer `fetch.min.bytes > 1` combined with `fetch.max.wait.ms > 100`.
- **throughput**: producer `linger.ms` below 5, `batch.size` below 64 KiB, no compression; consumer `fetch.min.bytes=1`.

### Spring Boot

If a file contains `spring.kafka.*` keys, it is split into a producer and a consumer config:

- `spring.kafka.properties.<kafka key>` applies to both; `spring.kafka.producer.properties.<kafka key>` and `spring.kafka.consumer.properties.<kafka key>` to one side.
- Plain-valued first-class keys are mapped: producer `acks`, `retries`, `batch-size`, `compression-type`, `client-id`, `key-serializer`, `value-serializer`; consumer `enable-auto-commit`, `auto-offset-reset`, `group-id`, `isolation-level`, `max-poll-records`, `client-id`, `key-deserializer`, `value-deserializer`.
- Duration and data-size keys (`heartbeat-interval`, `fetch-min-size`, ...) are not converted; they are listed as `not-interpreted` info findings.
- Spring Kafka listener containers set `enable.auto.commit=false` unless you set it, so the auto-commit check only fires for Spring configs that set it to `true`.

`application.yml` and environment-variable configs are not read.

## Features

- [x] Java `.properties` parsing (comments, `=`/`:`/space separators, line continuations)
- [x] Producer/consumer detection, `--type` override
- [x] Version-aware defaults (`--kafka-version`), never guessed when unknown
- [x] Durability, latency and throughput profiles with an explanation per finding
- [x] Idempotence conflict and silent-disable detection
- [x] Consumer heartbeat/session and poll-interval vs processing-time checks
- [x] Typo and wrong-client key detection
- [x] Spring Boot `spring.kafka.*` properties
- [x] Text and JSON output, CI exit codes
- [ ] `application.yml` and environment-variable configs
- [ ] librdkafka client defaults
- [ ] Topic-side checks (`min.insync.replicas`, replication factor) from `kafka-configs` output

## Limitations

- Kafka Java client semantics only.
- The tool never connects to a broker, so it cannot check topic `min.insync.replicas`, broker-side `group.consumer.*` settings or broker limits like `group.max.session.timeout.ms`.
- Typo detection only suggests a known key when the name is very close; unknown keys without a near match are not reported (plugins and interceptors add their own keys).
- Profile thresholds (e.g. linger above 10 ms for latency) are rules of thumb, not Kafka limits.

## Contributing

Issues and pull requests are welcome, especially corrections to defaults with a link to the Kafka docs or KIP. Please add a test with each rule change.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

Licensed under the [MIT License](LICENSE).
