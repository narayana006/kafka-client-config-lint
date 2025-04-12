import io
import json
from pathlib import Path

import pytest

from kafka_client_config_lint.cli import main
from kafka_client_config_lint.defaults import default_for, default_history, parse_version
from kafka_client_config_lint.loader import ClientConfig, LoadError, load, parse_properties
from kafka_client_config_lint.rules import lint

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def producer(**kv):
    return ClientConfig("t", {k.replace("_", "."): str(v) for k, v in kv.items()},
                        client_type="producer")


def consumer(**kv):
    return ClientConfig("t", {k.replace("_", "."): str(v) for k, v in kv.items()},
                        client_type="consumer")


def rules(findings, level=None):
    return sorted(f.rule for f in findings if level is None or f.level == level)


# --- properties parsing --------------------------------------------------------

def test_parse_properties_syntax():
    text = (
        "# comment\n! also comment\n"
        "a=1\n"
        "b : 2\n"
        "c 3\n"
        "  d=x \\\n"
        "     y\n"
        "e\\=f=g\n"
        "empty=\n"
    )
    values, lines = parse_properties(text)
    assert values == {"a": "1", "b": "2", "c": "3", "d": "x y", "e=f": "g", "empty": ""}
    assert lines["d"] == 6


def test_detect_type_and_ambiguous():
    assert load("acks=all\nlinger.ms=5\n", "p")[0].client_type == "producer"
    assert load("group.id=g\n", "c")[0].client_type == "consumer"
    with pytest.raises(LoadError):
        load("bootstrap.servers=b:9092\n", "x")
    assert load("bootstrap.servers=b:9092\n", "x", "consumer")[0].client_type == "consumer"


def test_spring_split_and_mapping():
    configs = load((EXAMPLES / "application.properties").read_text(), "app")
    by_type = {c.client_type: c for c in configs}
    assert by_type["producer"].values == {
        "security.protocol": "SASL_SSL", "acks": "all", "retries": "0",
        "enable.idempotence": "true"}
    assert by_type["consumer"].values["isolation.level"] == "read_committed"
    assert by_type["consumer"].not_interpreted == ["spring.kafka.consumer.heartbeat-interval"]


# --- defaults ------------------------------------------------------------------

def test_versioned_defaults():
    assert default_for("acks", (2, 8)) == "1"
    assert default_for("acks", (3, 0)) == "all"
    assert default_for("linger.ms", (3, 9)) == "0"
    assert default_for("linger.ms", (4, 0)) == "5"
    assert default_for("session.timeout.ms", (3, 7)) == "45000"
    assert default_for("acks", None) is None
    assert default_for("batch.size", None) == "16384"
    assert default_history("enable.idempotence") == "false before 3.0, true from 3.0"
    assert parse_version("3.7.1") == (3, 7)
    with pytest.raises(ValueError):
        parse_version("three")


# --- producer rules -------------------------------------------------------------

def test_acks_one_is_error_and_disables_idempotence():
    f = lint(producer(acks=1), version=(3, 7))
    assert rules(f, "error") == ["acks"]
    assert "idempotence-disabled" in rules(f, "warning")
    assert "ordering" in rules(f, "warning")


def test_unknown_version_does_not_assert_default():
    f = lint(producer(linger_ms=5))
    acks = [x for x in f if x.key == "acks"]
    assert acks and acks[0].level == "warning"
    assert "1 before 3.0, all from 3.0" in acks[0].why


def test_explicit_idempotence_conflict_is_error():
    f = lint(producer(enable_idempotence="true", acks="1"), version=(3, 7))
    assert "idempotence-conflict" in rules(f, "error")
    f = lint(producer(enable_idempotence="true", max_in_flight_requests_per_connection=6))
    assert "idempotence-conflict" in rules(f, "error")


def test_idempotence_off_explicitly_and_pre_3_default():
    assert "idempotence" in rules(lint(producer(acks="all", enable_idempotence="false")), "error")
    f = lint(producer(acks="all"), version=(2, 8))
    assert "idempotence" in rules(f, "warning")
    assert "ordering" in rules(f, "warning")


def test_durable_producer_is_clean():
    f = lint(producer(acks="all", enable_idempotence="true"), version=(3, 7))
    assert rules(f) == ["min-insync"]


def test_retries_zero():
    assert "retries" in rules(lint(producer(acks="all", retries=0, enable_idempotence="false")), "error")


def test_delivery_timeout_explicit_vs_default():
    f = lint(producer(acks="all", delivery_timeout_ms=1000), version=(3, 7))
    assert [x.level for x in f if x.rule == "delivery-timeout"] == ["error"]
    f = lint(producer(acks="all", request_timeout_ms=200000), version=(3, 7))
    assert [x.level for x in f if x.rule == "delivery-timeout"] == ["warning"]


def test_throughput_and_latency_profiles():
    f = lint(producer(acks="all"), profile="throughput", version=(3, 7))
    assert {"linger", "batch-size", "compression"} <= set(rules(f))
    f = lint(producer(acks="all"), profile="throughput", version=(4, 1))
    assert "linger" not in rules(f)  # 4.x default is 5
    f = lint(producer(acks="all"), profile="throughput")
    assert [x.level for x in f if x.rule == "linger"] == ["info"]
    f = lint(producer(linger_ms=100, compression_type="gzip"), profile="latency")
    assert rules(f) == ["compression", "linger"]


def test_invalid_values_and_typos():
    f = lint(producer(acks="most", linger_ms="soon", enable_idempotence="yes",
                      **{"batch_sise": "1"}))
    assert rules(f, "error").count("invalid-value") == 3
    assert any(x.rule == "unknown-key" and "batch.size" in x.message for x in f)
    f = lint(producer(acks="all", group_id="g"))
    assert "wrong-client" in rules(f)


# --- consumer rules -------------------------------------------------------------

def test_consumer_durability_defaults():
    f = lint(consumer(group_id="g"))
    assert rules(f) == ["auto-commit", "isolation", "offset-reset"]
    f = lint(consumer(group_id="g", enable_auto_commit="false",
                      auto_offset_reset="earliest", isolation_level="read_committed"))
    assert f == []


def test_spring_consumer_auto_commit_default_is_false():
    cfg = ClientConfig("s", {"group.id": "g"}, client_type="consumer", spring=True)
    assert "auto-commit" not in rules(lint(cfg))


def test_heartbeat_rules():
    assert "heartbeat" in rules(lint(consumer(session_timeout_ms=10000, heartbeat_interval_ms=10000)), "error")
    assert "heartbeat" in rules(lint(consumer(session_timeout_ms=10000, heartbeat_interval_ms=4000)), "warning")
    assert "heartbeat" not in rules(lint(consumer(session_timeout_ms=45000, heartbeat_interval_ms=3000)))
    # session default unknown without a version
    assert "heartbeat" in rules(lint(consumer(heartbeat_interval_ms=4000)), "info")
    assert "heartbeat" in rules(lint(consumer(heartbeat_interval_ms=4000), version=(2, 8)), "warning")
    f = lint(consumer(group_protocol="consumer", session_timeout_ms=45000))
    assert "group-protocol" in rules(f)


def test_poll_interval_vs_processing():
    assert "poll-interval" in rules(lint(consumer(), processing_ms=700), "error")
    assert "poll-interval" in rules(lint(consumer(), processing_ms=500), "warning")
    assert "poll-interval" not in rules(lint(consumer(max_poll_records=50), processing_ms=700))


def test_consumer_fetch_profiles():
    f = lint(consumer(fetch_min_bytes=65536, fetch_max_wait_ms=500), profile="latency")
    assert rules(f) == ["fetch-wait"]
    assert rules(lint(consumer(), profile="throughput")) == ["fetch-min"]


# --- CLI ------------------------------------------------------------------------

def test_cli_text_exit_codes(capsys):
    assert main([str(EXAMPLES / "producer.properties"), "--kafka-version", "3.7"]) == 1
    out = capsys.readouterr().out
    assert "compresion.type looks like a typo of compression.type (line 9)" in out
    assert main([str(EXAMPLES / "producer-durable.properties")]) == 0
    assert main([str(EXAMPLES / "producer-durable.properties"), "--fail-on", "info"]) == 1
    assert main([str(EXAMPLES / "producer.properties"), "--fail-on", "never"]) == 0


def test_cli_json(capsys):
    rc = main([str(EXAMPLES / "consumer.properties"), "--processing-ms", "800",
               "--format", "json"])
    data = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert data[0]["type"] == "consumer"
    assert {f["rule"] for f in data[0]["findings"]} >= {"poll-interval", "heartbeat"}


def test_cli_stdin_and_errors(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr("sys.stdin", io.StringIO("acks=all\nenable.idempotence=true\n"))
    assert main(["-"]) == 0
    assert main([str(tmp_path / "nope.properties")]) == 2
    assert main([str(EXAMPLES / "producer.properties"), "--kafka-version", "x"]) == 2
    ambiguous = tmp_path / "a.properties"
    ambiguous.write_text("bootstrap.servers=b:9092\n")
    assert main([str(ambiguous)]) == 2
    assert "--type" in capsys.readouterr().err
