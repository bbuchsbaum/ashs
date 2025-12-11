#!/bin/bash
#######################################################################
# ASHS Scheduler Adapter: LSF (IBM Spectrum LSF)
#######################################################################
# This adapter implements the scheduler API for IBM LSF.
#
# Required environment:
#   - LSF_BINDIR must be set
#   - bsub command available in PATH
#
# Implements:
#   scheduler_name()         - Returns "lsf"
#   scheduler_detect()       - Returns 0 if LSF is available
#   scheduler_submit()       - Submit a job, returns job ID
#   scheduler_submit_sync()  - Submit and wait for completion
#   scheduler_submit_array() - Submit array of jobs
#   scheduler_wait()         - Wait for job(s) to complete
#   scheduler_build_opts()   - Convert generic options to LSF flags
#######################################################################

SCHEDULER_NAME="lsf"

function scheduler_name() {
  echo "lsf"
}

function scheduler_detect() {
  [[ -n "$LSF_BINDIR" ]] && command -v bsub >/dev/null 2>&1
}

# Convert generic resource options to LSF flags
# Args: memory cores walltime queue
function scheduler_build_opts() {
  local memory="$1"
  local cores="$2"
  local walltime="$3"
  local queue="$4"
  local opts=""

  # Convert memory (e.g., "8G" -> "8000" for rusage)
  if [[ -n "$memory" ]]; then
    local mem_mb
    if [[ "$memory" =~ ^([0-9]+)G$ ]]; then
      mem_mb=$((${BASH_REMATCH[1]} * 1000))
    elif [[ "$memory" =~ ^([0-9]+)M$ ]]; then
      mem_mb="${BASH_REMATCH[1]}"
    else
      mem_mb="$memory"
    fi
    opts+=" -R 'rusage[mem=${mem_mb}]'"
  fi

  [[ -n "$cores" ]] && opts+=" -n $cores"

  # Convert walltime (e.g., "4:00:00" -> "4:00" for LSF)
  if [[ -n "$walltime" ]]; then
    local lsf_time
    if [[ "$walltime" =~ ^([0-9]+):([0-9]+):([0-9]+)$ ]]; then
      lsf_time="${BASH_REMATCH[1]}:${BASH_REMATCH[2]}"
    else
      lsf_time="$walltime"
    fi
    opts+=" -W $lsf_time"
  fi

  [[ -n "$queue" ]] && opts+=" -q $queue"

  # Add any extra options from config
  [[ -n "$ASHS_LSF_EXTRA_OPTS" ]] && opts+=" $ASHS_LSF_EXTRA_OPTS"

  # Add email notifications if configured
  if [[ -n "$ASHS_NOTIFY_EMAIL" ]]; then
    opts+=" -u $ASHS_NOTIFY_EMAIL"
    case "${ASHS_NOTIFY_EVENTS:-fail}" in
      all|fail|end|begin) opts+=" -N" ;;
      none) ;;
    esac
  fi

  echo "$opts"
}

# Submit a single job and return job ID
# Args: script [opts] [args...]
function scheduler_submit() {
  local script="$1"
  shift
  local opts="$QOPTS"
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  local job_name="${ASHS_JOB_PREFIX:-ashs}_$(basename "$script" .sh)"

  mkdir -p "$log_dir"

  local output
  output=$(bsub $opts \
    -J "$job_name" \
    -o "$log_dir/${job_name}.o%J" \
    -e "$log_dir/${job_name}.e%J" \
    -cwd "$PWD" \
    "$script" "$@" 2>&1)

  # Extract job ID from "Job <12345> is submitted to queue <normal>."
  local job_id
  job_id=$(echo "$output" | grep -oP '(?<=Job <)\d+(?=>)')

  echo "$job_id"
}

# Submit a job and wait for completion
function scheduler_submit_sync() {
  local script="$1"
  shift
  local opts="$QOPTS"
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  local job_name="${ASHS_JOB_PREFIX:-ashs}_$(basename "$script" .sh)"

  mkdir -p "$log_dir"

  # -K flag makes bsub wait for completion
  bsub -K $opts \
    -J "$job_name" \
    -o "$log_dir/${job_name}.o%J" \
    -e "$log_dir/${job_name}.e%J" \
    -cwd "$PWD" \
    "$script" "$@"
}

# Submit jobs for each parameter value
# Args: name params script [args...]
function scheduler_submit_array_single() {
  local name="$1"
  local params="$2"
  local script="$3"
  shift 3
  local opts="$QOPTS"
  local log_dir="${ASHS_WORK:-$(pwd)}/dump"

  mkdir -p "$log_dir"

  local job_ids=()
  for p in $params; do
    local job_name="${ASHS_JOB_PREFIX:-ashs}_${name}_${p}"
    local output
    output=$(bsub $opts \
      -J "$job_name" \
      -o "$log_dir/${job_name}.o%J" \
      -e "$log_dir/${job_name}.e%J" \
      -cwd "$PWD" \
      "$script" "$@" "$p" 2>&1)

    local job_id
    job_id=$(echo "$output" | grep -oP '(?<=Job <)\d+(?=>)')
    job_ids+=("$job_id")
  done

  echo "${job_ids[*]}"
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

  local job_ids=()
  for p1 in $params1; do
    for p2 in $params2; do
      local job_name="${ASHS_JOB_PREFIX:-ashs}_${name}_${p1}_${p2}"
      local output
      output=$(bsub $opts \
        -J "$job_name" \
        -o "$log_dir/${job_name}.o%J" \
        -e "$log_dir/${job_name}.e%J" \
        -cwd "$PWD" \
        "$script" "$@" "$p1" "$p2" 2>&1)

      local job_id
      job_id=$(echo "$output" | grep -oP '(?<=Job <)\d+(?=>)')
      job_ids+=("$job_id")
    done
  done

  echo "${job_ids[*]}"
}

# Wait for job(s) to complete
# Args: job_ids (space-separated)
function scheduler_wait() {
  local job_ids="$1"

  for job_id in $job_ids; do
    [[ -z "$job_id" ]] && continue

    # Use dependency to wait for job completion
    bsub -K -o /dev/null -w "ended($job_id)" /bin/sleep 1 2>/dev/null
  done
}

# Get number of available CPUs in current job
function scheduler_get_slots() {
  echo "${LSB_MAX_NUM_PROCESSORS:-1}"
}

# Check if running inside an LSF job
function scheduler_in_job() {
  [[ -n "$LSB_JOBID" ]]
}
