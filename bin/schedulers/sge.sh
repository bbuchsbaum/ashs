#!/bin/bash
#######################################################################
# ASHS Scheduler Adapter: SGE (Sun Grid Engine / Open Grid Scheduler)
#######################################################################
# This adapter implements the scheduler API for SGE/OGS.
#
# Required environment:
#   - SGE_ROOT must be set
#   - qsub command available in PATH
#
# Implements:
#   scheduler_name()         - Returns "sge"
#   scheduler_detect()       - Returns 0 if SGE is available
#   scheduler_submit()       - Submit a job, returns job ID
#   scheduler_submit_sync()  - Submit and wait for completion
#   scheduler_submit_array() - Submit array of jobs
#   scheduler_wait()         - Wait for job(s) to complete
#   scheduler_build_opts()   - Convert generic options to SGE flags
#######################################################################

SCHEDULER_NAME="sge"

function scheduler_name() {
  echo "sge"
}

function scheduler_detect() {
  [[ -n "$SGE_ROOT" ]] && command -v qsub >/dev/null 2>&1
}

# Convert generic resource options to SGE flags
# Args: memory cores walltime queue
function scheduler_build_opts() {
  local memory="$1"
  local cores="$2"
  local walltime="$3"
  local queue="$4"
  local opts=""

  [[ -n "$memory" ]] && opts+=" -l h_vmem=$memory"
  [[ -n "$cores" ]] && opts+=" -pe smp $cores"
  [[ -n "$walltime" ]] && opts+=" -l h_rt=$walltime"
  [[ -n "$queue" ]] && opts+=" -q $queue"

  # Add any extra options from config
  [[ -n "$ASHS_SGE_EXTRA_OPTS" ]] && opts+=" $ASHS_SGE_EXTRA_OPTS"

  # Add email notifications if configured
  if [[ -n "$ASHS_NOTIFY_EMAIL" ]]; then
    opts+=" -M $ASHS_NOTIFY_EMAIL"
    case "${ASHS_NOTIFY_EVENTS:-fail}" in
      all)   opts+=" -m beas" ;;
      fail)  opts+=" -m a" ;;
      end)   opts+=" -m e" ;;
      begin) opts+=" -m b" ;;
      none)  ;;
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
  output=$(qsub $opts \
    -N "$job_name" \
    -j y \
    -o "$log_dir" \
    -cwd \
    -V \
    "$script" "$@" 2>&1)

  # Extract job ID from "Your job 12345 has been submitted"
  local job_id
  job_id=$(echo "$output" | grep -oP '(?<=Your job )\d+' | head -1)

  if [[ -z "$job_id" ]]; then
    job_id=$(echo "$output" | awk '/Your job/ {print $3}')
  fi

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

  qsub -sync y $opts \
    -N "$job_name" \
    -j y \
    -o "$log_dir" \
    -cwd \
    -V \
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
    output=$(qsub $opts \
      -N "$job_name" \
      -j y \
      -o "$log_dir" \
      -cwd \
      -V \
      "$script" "$@" "$p" 2>&1)

    local job_id
    job_id=$(echo "$output" | awk '/Your job/ {print $3}')
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
      output=$(qsub $opts \
        -N "$job_name" \
        -j y \
        -o "$log_dir" \
        -cwd \
        -V \
        "$script" "$@" "$p1" "$p2" 2>&1)

      local job_id
      job_id=$(echo "$output" | awk '/Your job/ {print $3}')
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

    # Use a dependency job to wait
    qsub -b y -sync y -j y -o /dev/null -hold_jid "$job_id" /bin/sleep 1 2>/dev/null
  done
}

# Get number of available CPUs in current job
function scheduler_get_slots() {
  echo "${NSLOTS:-1}"
}

# Check if running inside an SGE job
function scheduler_in_job() {
  [[ -n "$JOB_ID" ]]
}
