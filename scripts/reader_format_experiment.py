#!/usr/bin/env python3
"""Compare ephemeral row/column/zstd codecs through real immutable execution.

Creates independent snapshots and targets; never connects to PostgreSQL or
publishes objects. All objects use the in-process InMemory adapter. Candidate
codecs are deliberately experimental and are never copied into production src.
"""
import argparse
import io
import json
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import subprocess
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def replace(path, old, new):
    text = path.read_text()
    if text.count(old) != 1:
        raise RuntimeError(f"expected exactly one experiment hook in {path}: {old}")
    path.write_text(text.replace(old, new))


def snapshot(destination, revision=None):
    destination.mkdir()
    if revision:
        archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(archive)) as files:
            files.extractall(destination, filter="data")
    else:
        files = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
        for relative in files.decode().split("\0"):
            if relative:
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, target)


def candidate(source):
    cargo = source / "Cargo.toml"
    replace(cargo, "[dependencies]", '[dependencies]\nbincode = "1.3"\nzstd = "0.13"')
    reader = source / "src/engine/reader"
    shutil.copy2(ROOT / "scripts/reader_format_experiment/codec.rs", reader / "experiment_codec.rs")
    replace(reader / "mod.rs", "mod bounded;", "mod bounded;\nmod experiment_codec;")
    replace(reader / "object.rs", "let chunk = rows.by_ref().take(block_rows.min(65_536)).collect::<Vec<_>>();", "let chunk = rows.by_ref().take(block_rows.min(65_536)).map(|(tuple, weight)| (tuple.clone(), *weight)).collect::<Vec<_>>();")
    replace(reader / "object.rs", "let encoded = super::bounded::encode(&chunk)?;", "let encoded = super::experiment_codec::encode(&chunk)?;")
    replace(reader / "object.rs", "let rows: Vec<Entry<K, V>> = serde_json::from_slice(&bytes)?;", "let rows: Vec<Entry<K, V>> = super::experiment_codec::decode(&bytes)?;")
    replace(reader / "streaming.rs", "let encoded = bounded::encode(&self.rows)?;", "let encoded = super::experiment_codec::encode(&self.rows)?;")


def build(source, work, label):
    environment = dict(os.environ, CARGO_TARGET_DIR=str(work / f"target-{label}"))
    command = ["cargo", "test", "--release", "--lib", "reader_execution_measurement", "--no-run", "--message-format=json"]
    result = subprocess.run(command, cwd=source, env=environment, text=True, capture_output=True)
    (work / f"build-{label}.log").write_text(result.stdout + result.stderr)
    result.check_returncode()
    for line in reversed(result.stdout.splitlines()):
        record = json.loads(line)
        if record.get("reason") == "compiler-artifact" and record.get("executable"):
            return record["executable"]
    raise RuntimeError(f"missing benchmark executable for {label}")


def run(cases, work, repeat):
    order = cases * repeat
    random.Random(712).shuffle(order)
    records = []
    for index, (label, executable, mode) in enumerate(order):
        environment = dict(os.environ, PGDERIVE_EXPERIMENT_CODEC=mode)
        result = subprocess.run([executable, "reader_execution_measurement", "--ignored", "--nocapture"], env=environment, text=True, capture_output=True)
        (work / f"run-{index:02}-{label}.log").write_text(result.stdout + result.stderr)
        result.check_returncode()
        measures = {key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", result.stdout)}
        records.append({"case": label, "measurements": measures})
        (work / "records.json").write_text(json.dumps(records, indent=2))
    summary = {}
    for label, _, _ in cases:
        samples = [record["measurements"] for record in records if record["case"] == label]
        summary[label] = {key: {"median": statistics.median(sample[key] for sample in samples), "min": min(sample[key] for sample in samples), "max": max(sample[key] for sample in samples)} for key in samples[0]}
    (work / "summary.json").write_text(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default="d7368d1")
    parser.add_argument("--repeat", default=7, type=int)
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be positive")
    work = args.work_dir or Path(tempfile.mkdtemp(prefix="pgderive-format-"))
    work.mkdir(parents=True, exist_ok=True)
    before, after, experiment = (work / name for name in ("before", "after", "experiment"))
    snapshot(before, args.baseline_ref)
    snapshot(after)
    snapshot(experiment)
    shutil.copy2(after / "src/engine/reader/tests/performance.rs", before / "src/engine/reader/tests/performance.rs")
    with (before / "src/engine/reader/mod.rs").open("a") as module:
        module.write('\n#[cfg(test)]\n#[path = "tests/performance.rs"]\nmod performance;\n')
    candidate(experiment)
    cases = [("production-before", build(before, work, "before"), "json"), ("production-after", build(after, work, "after"), "json")]
    binary = build(experiment, work, "experiment")
    contract = subprocess.run([binary, "generic_serde_contract_requires_json_fallback", "--nocapture"], text=True, capture_output=True)
    (work / "generic-serde-contract.log").write_text(contract.stdout + contract.stderr)
    contract.check_returncode()
    cases.extend((f"format-{mode}", binary, mode) for mode in ("json", "binary", "column", "json-zstd", "binary-zstd", "column-zstd"))
    run(cases, work, args.repeat)
    print(work / "summary.json")


if __name__ == "__main__":
    main()
