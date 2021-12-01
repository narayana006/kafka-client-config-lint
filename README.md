# kafka-client-config-lint

Lint Kafka producer and consumer configs against durability, latency or throughput profiles, with an explanation for every finding.

## Planned scope

Read client .properties files (and Spring/env-style keys if simple), detect whether producer or consumer, check against a chosen profile (durability: acks=all, enable.idempotence, retries, max.in.flight<=5, min.insync awareness; consumer: enable.auto.commit, max.poll.interval.ms vs processing time, isolation.level, session/heartbeat ratio; throughput: linger.ms, batch.size, compression.type). Be accurate about Kafka client defaults by version (idempotence default true since 3.0 producer); if unsure of a default, do not assert it. Text/JSON output, exit codes.
