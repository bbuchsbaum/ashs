#!/bin/bash
#######################################################################
# ASHS Scheduler Adapter: Local (Sequential Execution)
#######################################################################
# This adapter implements the scheduler API for local sequential
# execution without any parallelism. Useful for testing, debugging,
# or systems without a scheduler.
#
# Required:
#   - Nothing (always available)
#
# Implements:
#   scheduler_name()         - Returns "local"
#   scheduler_detect()       - Always returns 0
#   scheduler_submit()       - Execute job directly, returns 0
#   scheduler_submit_sync()  - Execute job directly
#   scheduler_submit_array() - Execute jobs sequentially
#   scheduler_wait()         - No-op (jobs already complete)
#   scheduler_build_opts()   - Returns empty (no options needed)
#######################################################################

SCHEDULER_NAME="local"

function scheduler_name() {
  echo "local"
}

function scheduler_detect() {
  # Always available
  return 0
}

# No options for local execution
function scheduler_build_opts() {
  echo ""
}

# Submit a single job (execute directly)
# Args: script [args...]
function scheduler_submit() {
  local script="$1"
  shift
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  local job_name="${ASHS_JOB_PREFIX:-ashs}_$(basename "$script" .sh)"

  mkdir -p "$log_dir"

  # Execute in background with logging
  {
    echo "=== Job started at $(date) ==="
    echo "Script: $script"
    echo "Args: $@"
    echo "==================================="
    bash "$script" "$@"
    local exit_code=$?
    echo "==================================="
    echo "=== Job finished at $(date) with exit code $exit_code ==="
  } > "$log_dir/${job_name}_$$.log" 2>&1 &

  local pid=$!
  echo "$pid"
}

# Submit a job and wait for completion (execute directly)
function scheduler_submit_sync() {
  local script="$1"
  shift
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  local job_name="${ASHS_JOB_PREFIX:-ashs}_$(basename "$script" .sh)"

  mkdir -p "$log_dir"

  # Execute with logging
  {
    echo "=== Job started at $(date) ==="
    echo "Script: $script"
    echo "Args: $@"
    echo "==================================="
    bash "$script" "$@"
    local exit_code=$?
    echo "==================================="
    echo "=== Job finished at $(date) with exit code $exit_code ==="
    return $exit_code
  } 2>&1 | tee "$log_dir/${job_name}_$$.log"

  return ${PIPESTATUS[0]}
}

# Submit jobs for each parameter value (execute sequentially)
# Args: name params script [args...]
function scheduler_submit_array_single() {
  local name="$1"
  local params="$2"
  local script="$3"
  shift 3
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"

  mkdir -p "$log_dir"

  for p in $params; do
    local job_name="${ASHS_JOB_PREFIX:-ashs}_${name}_${p}"
    echo "Running: $script $@ $p"

    {
      echo "=== Job started at $(date) ==="
      bash "$script" "$@" "$p"
      local exit_code=$?
      echo "=== Job finished at $(date) with exit code $exit_code ==="
    } > "$log_dir/${job_name}.log" 2>&1

    if [[ $exit_code -ne 0 ]]; then
      echo "Warning: Job $job_name exited with code $exit_code" >&2
    fi
  done

  echo "0"  # Return dummy job ID
}

# Submit jobs for each combination of two parameters
# Args: name params1 params2 script [args...]
function scheduler_submit_array_double() {
  local name="$1"
  local params1="$2"
  local params2="$3"
  local script="$4"
  shift 4
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"

  mkdir -p "$log_dir"

  for p1 in $params1; do
    for p2 in $params2; do
      local job_name="${ASHS_JOB_PREFIX:-ashs}_${name}_${p1}_${p2}"
      echo "Running: $script $@ $p1 $p2"

      {
        echo "=== Job started at $(date) ==="
        bash "$script" "$@" "$p1" "$p2"
        local exit_code=$?
        echo "=== Job finished at $(date) with exit code $exit_code ==="
      } > "$log_dir/${job_name}.log" 2>&1

      if [[ $exit_code -ne 0 ]]; then
        echo "Warning: Job $job_name exited with code $exit_code" >&2
      fi
    done
  done

  echo "0"  # Return dummy job ID
}

# Wait for job(s) to complete
# Args: job_ids (PIDs or dummy IDs)
function scheduler_wait() {
  local pids="$1"

  for pid in $pids; do
    [[ -z "$pid" || "$pid" == "0" ]] && continue
    wait "$pid" 2>/dev/null
  done
}

# Get number of available CPUs
function scheduler_get_slots() {
  echo "1"
}

# Check if running inside a scheduled job
function scheduler_in_job() {
  return 1  # Never in a job for local execution
}
