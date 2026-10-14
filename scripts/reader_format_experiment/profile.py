#!/usr/bin/env python3
"""Profile CPU and aggregate allocations in unpublished temporary reader fixtures.

Uses GNU time for uninstrumented CPU; pinned dhat 0.3.3 supplies a safe allocator
in separate temporary source snapshots/targets. Production sources/dependencies
are never edited. See docs/reader-storage-performance.md for measurement scopes.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import random
import re
import shutil
import subprocess
import statistics
import tempfile


RUNNER = Path(__file__).resolve().parents[1] / "reader_format_experiment.py"
spec = importlib.util.spec_from_file_location("reader_format_experiment", RUNNER)
experiment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(experiment)


def register_fixture(before, after):
    relative = "src/engine/reader/tests/performance.rs"
    shutil.copy2(after / relative, before / relative)
    module = before / "src/engine/reader/mod.rs"
    text = module.read_text()
    if "mod performance;" not in text:
        module.write_text(text + '\n#[cfg(test)]\n#[path = "tests/performance.rs"]\nmod performance;\n')


def instrument(source):
    experiment.replace(source / "Cargo.toml", "[dependencies]", '[dependencies]\ndhat = "=0.3.3"')
    with (source / "src/lib.rs").open("a") as library:
        library.write('\n#[cfg(test)]\n#[global_allocator]\nstatic PROFILING_ALLOCATOR: dhat::Alloc = dhat::Alloc;\n')
    fixture = source / "src/engine/reader/tests/performance.rs"
    text = fixture.read_text()
    entry = "async fn reader_execution_measurement() -> Result<()> {"
    if text.count(entry) != 1:
        raise RuntimeError("expected one reader benchmark entry")
    text = text.replace(entry, entry + '\n    let _profile = dhat::Profiler::builder().testing().trim_backtraces(Some(4)).build();')
    end = text.index("    Ok(())", text.index(entry))
    text = text[:end] + '''    let profile_stats = dhat::HeapStats::get();
    println!("allocation_profile_total_blocks={};allocation_profile_total_bytes={};allocation_profile_peak_blocks={};allocation_profile_peak_bytes={};allocation_profile_live_blocks={};allocation_profile_live_bytes={}", profile_stats.total_blocks, profile_stats.total_bytes, profile_stats.max_blocks, profile_stats.max_bytes, profile_stats.curr_blocks, profile_stats.curr_bytes);
''' + text[end:]
    fixture.write_text(text)


def cpu_sample(binary, report, output):
    command = ["/usr/bin/time", "-v", "-o", str(report), binary, "reader_execution_measurement", "--ignored", "--nocapture"]
    result = subprocess.run(command, text=True, capture_output=True)
    output.write_text(result.stdout + result.stderr)
    result.check_returncode()
    names = {
        r"User time \(seconds\)": "user_seconds",
        r"System time \(seconds\)": "system_seconds",
        r"Maximum resident set size \(kbytes\)": "max_rss_kib",
        r"Minor \(reclaiming a frame\) page faults": "minor_faults",
        r"Major \(requiring I/O\) page faults": "major_faults",
        "Voluntary context switches": "voluntary_context_switches",
        "Involuntary context switches": "involuntary_context_switches",
    }
    text = report.read_text()
    measures = {key: float(re.search(name + r":\s*([0-9.]+)", text).group(1)) for name, key in names.items()}
    measures["cpu_seconds"] = measures["user_seconds"] + measures["system_seconds"]
    return measures


def allocation_sample(binary, output):
    command = [binary, "reader_execution_measurement", "--ignored", "--nocapture", "--test-threads=1"]
    result = subprocess.run(command, text=True, capture_output=True)
    output.write_text(result.stdout + result.stderr)
    result.check_returncode()
    measures = {key: int(value) for key, value in re.findall(r"allocation_profile_(\w+)=(\d+)", result.stdout)}
    if len(measures) != 6:
        raise RuntimeError("missing allocator counter output")
    return measures


def profile(binaries, work, kind, repeat, seed):
    order = list(binaries) * repeat
    random.Random(seed).shuffle(order)
    records = []
    for index, label in enumerate(order):
        prefix = work / f"{kind}-{index:02}-{label}"
        if kind == "cpu":
            measures = cpu_sample(binaries[label], prefix.with_suffix(".time"), prefix.with_suffix(".log"))
        else:
            measures = allocation_sample(binaries[label], prefix.with_suffix(".log"))
        records.append({"case": label, "measurements": measures})
        (work / f"{kind}-records.json").write_text(json.dumps(records, indent=2))
    summary = {}
    for label in binaries:
        samples = [record["measurements"] for record in records if record["case"] == label]
        summary[label] = {key: {"median": statistics.median(sample[key] for sample in samples), "min": min(sample[key] for sample in samples), "max": max(sample[key] for sample in samples)} for key in samples[0]}
    return {"records": records, "summary": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default="d7368d1")
    parser.add_argument("--cpu-repeat", default=15, type=int)
    parser.add_argument("--allocation-repeat", default=3, type=int)
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if min(args.cpu_repeat, args.allocation_repeat) < 1:
        parser.error("repeat counts must be positive")
    if not Path("/usr/bin/time").exists():
        parser.error("GNU /usr/bin/time is required")
    work = args.work_dir or Path(tempfile.mkdtemp(prefix="pgderive-reader-profile-"))
    work.mkdir(parents=True, exist_ok=True)
    before, after = work / "before", work / "after"
    experiment.snapshot(before, args.baseline_ref)
    experiment.snapshot(after)
    register_fixture(before, after)
    sources = {"before": before, "after": after}
    binaries = {label: experiment.build(source, work, label) for label, source in sources.items()}
    tools = {name: shutil.which(name) for name in ("perf", "heaptrack", "valgrind")}
    paranoid = Path("/proc/sys/kernel/perf_event_paranoid")
    tools["perf_event_paranoid"] = paranoid.read_text().strip() if paranoid.exists() else None
    if tools["perf"]:
        result = subprocess.run([tools["perf"], "stat", "-e", "task-clock,cycles,instructions", binaries["before"], "reader_execution_measurement", "--ignored", "--nocapture"], capture_output=True, text=True)
        (work / "perf.log").write_text(result.stdout + result.stderr)
        tools["perf_exit_code"] = result.returncode
    cpu = profile(binaries, work, "cpu", args.cpu_repeat, 713)
    for source in sources.values():
        instrument(source)
    instrumented = {label: experiment.build(source, work, label + "-dhat") for label, source in sources.items()}
    allocations = profile(instrumented, work, "allocations", args.allocation_repeat, 714)
    result = {"tools": tools, "dhat": "0.3.3", "cpu": cpu, "allocations": allocations}
    (work / "profile.json").write_text(json.dumps(result, indent=2))
    print(work / "profile.json")


if __name__ == "__main__":
    main()
