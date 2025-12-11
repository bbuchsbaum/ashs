#!/bin/bash
#######################################################################
# ASHS Scheduler Adapter: GNU Parallel
#######################################################################
# This adapter implements the scheduler API for GNU Parallel.
# Useful for local multi-core execution without a cluster scheduler.
#
# Required:
#   - GNU parallel command available in PATH
#
# Implements:
#   scheduler_name()         - Returns "parallel"
#   scheduler_detect()       - Returns 0 if GNU Parallel is available
#   scheduler_submit()       - Submit a job, returns PID
#   scheduler_submit_sync()  - Submit and wait for completion
#   scheduler_submit_array() - Submit array of jobs via parallel
#   scheduler_wait()         - Wait for job(s) to complete
#   scheduler_build_opts()   - Convert generic options to parallel flags
#######################################################################

SCHEDULER_NAME="parallel"

function scheduler_name() {
  echo "parallel"
}

function scheduler_detect() {
  command -v parallel >/dev/null 2>&1
}

# Convert generic resource options to GNU Parallel flags
# Args: memory cores walltime queue
function scheduler_build_opts() {
  local memory="$1"   # Not used by parallel
  local cores="$2"
  local walltime="$3" # Not used by parallel
  local queue="$4"    # Not used by parallel
  local opts=""

  # Number of parallel jobs
  if [[ -n "$cores" ]]; then
    opts+=" -j $cores"
  else
    # Default to number of CPU cores
    opts+=" -j $(nproc 2>/dev/null || echo 4)"
  fi

  # Add any extra options from config
  [[ -n "$ASHS_PARALLEL_EXTRA_OPTS" ]] && opts+=" $ASHS_PARALLEL_EXTRA_OPTS"

  echo "$opts"
}

# Helper function to wrap job execution for parallel
function _parallel_job_wrapper() {
  local job_name="$1"
  local log_dir="$2"
  shift 2

  # Create log file
  local log_file="$log_dir/${job_name}_$$.log"

  # Execute and capture output
  {
    echo "=== Job started at $(date) ==="
    echo "Command: $@"
    echo "==================================="
    "$@"
    local exit_code=$?
    echo "==================================="
    echo "=== Job finished at $(date) with exit code $exit_code ==="
  } > "$log_file" 2>&1

  return $exit_code
}

# Submit a single job and return PID
# Args: script [args...]
function scheduler_submit() {
  local script="$1"
  shift
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  local job_name="${ASHS_JOB_PREFIX:-ashs}_$(basename "$script" .sh)"

  mkdir -p "$log_dir"

  # Run in background
  _parallel_job_wrapper "$job_name" "$log_dir" bash "$script" "$@" &
  local pid=$!

  echo "$pid"
}

# Submit a job and wait for completion
function scheduler_submit_sync() {
  local script="$1"
  shift
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  local job_name="${ASHS_JOB_PREFIX:-ashs}_$(basename "$script" .sh)"

  mkdir -p "$log_dir"

  # Run and wait
  _parallel_job_wrapper "$job_name" "$log_dir" bash "$script" "$@"
}

# Submit jobs for each parameter value using GNU Parallel
# Args: name params script [args...]
function scheduler_submit_array_single() {
  local name="$1"
  local params="$2"
  local script="$3"
  shift 3
  local opts="$QOPTS"
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"

  mkdir -p "$log_dir"

  # Export necessary variables for parallel
  export ASHS_WORK ASHS_ROOT ASHS_CONFIG
  export -f _parallel_job_wrapper 2>/dev/null || true

  # Run all jobs in parallel
  parallel $opts \
    --joblog "$log_dir/${name}_parallel.log" \
    --results "$log_dir/${name}_{}" \
    bash "$script" "$@" {} ::: $params &

  local pid=$!
  echo "$pid"
}

# Submit jobs for each combination of two parameters
# Args: name params1 params2 script [args...]
function scheduler_submit_array_double() {
  local name="$1"
  local params1="$2"
  local params2="$3"
  local script="$4"
  shift 4
  local opts="$QOPTS"
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"

  mkdir -p "$log_dir"

  # Export necessary variables for parallel
  export ASHS_WORK ASHS_ROOT ASHS_CONFIG

  # Run all combinations in parallel
  parallel $opts \
    --joblog "$log_dir/${name}_parallel.log" \
    --results "$log_dir/${name}_{1}_{2}" \
    bash "$script" "$@" {1} {2} ::: $params1 ::: $params2 &

  local pid=$!
  echo "$pid"
}

# Wait for job(s) to complete
# Args: job_ids (space-separated PIDs)
function scheduler_wait() {
  local pids="$1"

  for pid in $pids; do
    [[ -z "$pid" ]] && continue
    wait "$pid" 2>/dev/null
  done
}

# Get number of available CPUs
function scheduler_get_slots() {
  nproc 2>/dev/null || echo "1"
}

# Check if running inside a parallel job
function scheduler_in_job() {
  [[ -n "$PARALLEL_SEQ" ]]
}
