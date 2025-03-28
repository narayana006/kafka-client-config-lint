"""Command-line interface for kafka-client-config-lint."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .defaults import parse_version
from .loader import ClientConfig, LoadError, load
from .rules import LEVELS, PROFILES, Finding, lint

EXIT_OK, EXIT_FINDINGS, EXIT_ERROR = 0, 1, 2


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-client-config-lint",
        description="Lint Kafka producer/consumer .properties files (and Spring Boot "
                    "spring.kafka.* properties) against a durability, latency or "
                    "throughput profile.",
        epilog="exit codes: 0 no findings at or above --fail-on, 1 findings, 2 input error",
    )
    p.add_argument("files", nargs="+", help="client .properties files ('-' for stdin)")
    p.add_argument("--profile", choices=PROFILES, default="durability")
    p.add_argument("--type", choices=("producer", "consumer"), dest="client_type",
                   help="client type (default: detect from the keys)")
    p.add_argument("--kafka-version", metavar="X.Y",
                   help="client library version, used to resolve defaults that "
                        "changed between releases")
    p.add_argument("--processing-ms", type=float, metavar="MS",
                   help="worst-case processing time per record, checked against "
                        "max.poll.records and max.poll.interval.ms")
    p.add_argument("--format", choices=("text", "json"), default="text")
    p.add_argument("--fail-on", choices=LEVELS + ("never",), default="error",
                   help="lowest level that makes the exit code 1 (default: error)")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def format_text(results: list[tuple[ClientConfig, list[Finding]]], profile: str) -> str:
    out = []
    total = {lvl: 0 for lvl in LEVELS}
    for cfg, findings in results:
        out.append(f"{cfg.source}: {cfg.client_type}, profile {profile}")
        if not findings:
            out.append("  no findings")
        for f in findings:
            total[f.level] += 1
            where = f" (line {f.line})" if f.line else ""
            out.append(f"  {f.level.upper():<7} {f.rule}: {f.message}{where}")
            out.append(f"          {f.why}")
        out.append("")
    out.append(", ".join(f"{total[lvl]} {lvl}" for lvl in reversed(LEVELS)))
    return "\n".join(out)


def format_json(results, profile: str) -> str:
    return json.dumps([
        {"source": cfg.source, "type": cfg.client_type, "profile": profile,
         "findings": [f.to_dict() for f in findings]}
        for cfg, findings in results
    ], indent=2)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        version = parse_version(args.kafka_version) if args.kafka_version else None
        configs: list[ClientConfig] = []
        for path in args.files:
            configs.extend(load(_read(path), "stdin" if path == "-" else path,
                                args.client_type))
    except (OSError, ValueError) as exc:  # LoadError is a ValueError
        print(f"kafka-client-config-lint: error: {exc}", file=sys.stderr)
        return EXIT_ERROR

    results = [(c, lint(c, args.profile, version, args.processing_ms)) for c in configs]
    print(format_json(results, args.profile) if args.format == "json"
          else format_text(results, args.profile))
    if args.fail_on == "never":
        return EXIT_OK
    threshold = LEVELS.index(args.fail_on)
    failing = any(LEVELS.index(f.level) >= threshold for _, fs in results for f in fs)
    return EXIT_FINDINGS if failing else EXIT_OK


__all__ = ["main", "LoadError"]

if __name__ == "__main__":
    sys.exit(main())
