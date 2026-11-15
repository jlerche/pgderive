#!/usr/bin/env bash
# Join all checks, including failed peers that need to retain evidence.
run_parallel_checks() {
    local check pid status=0 failed=0
    local -a pids=()
    for check in "$@"; do
        "$check" &
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do
        failed=0
        wait "$pid" || failed=$?
        if (( status == 0 )); then
            status=$failed
        fi
    done
    return "$status"
}
