#!/bin/bash
#######################################################################
# ASHS Scheduler Adapter: SLURM
#######################################################################
# This adapter implements the scheduler API for SLURM workload manager.
#
# Required environment:
#   - sbatch, squeue, sacct commands available in PATH
#
# Implements:
#   scheduler_name()         - Returns "slurm"
#   scheduler_detect()       - Returns 0 if SLURM is available
#   scheduler_submit()       - Submit a job, returns job ID
#   scheduler_submit_sync()  - Submit and wait for completion
#   scheduler_submit_array() - Submit array of jobs
#   scheduler_wait()         - Wait for job(s) to complete
#   scheduler_build_opts()   - Convert generic options to SLURM flags
#######################################################################

SCHEDULER_NAME="slurm"

function scheduler_name() {
  echo "slurm"
}

function scheduler_detect() {
  command -v sbatch >/dev/null 2>&1 && \
  command -v squeue >/dev/null 2>&1 && \
  command -v sacct >/dev/null 2>&1
}

# Convert generic resource options to SLURM flags
# Args: memory cores walltime queue
function scheduler_build_opts() {
  local memory="$1"
  local cores="$2"
  local walltime="$3"
  local queue="$4"
  local opts=""

  [[ -n "$memory" ]] && opts+=" --mem=$memory"
  [[ -n "$cores" ]] && opts+=" --cpus-per-task=$cores"
  [[ -n "$walltime" ]] && opts+=" --time=$walltime"
  [[ -n "$queue" ]] && opts+=" --partition=$queue"

  # Add any extra options from config
  [[ -n "$ASHS_SLURM_EXTRA_OPTS" ]] && opts+=" $ASHS_SLURM_EXTRA_OPTS"

  # Add email notifications if configured
  if [[ -n "$ASHS_NOTIFY_EMAIL" ]]; then
    opts+=" --mail-user=$ASHS_NOTIFY_EMAIL"
    case "${ASHS_NOTIFY_EVENTS:-fail}" in
      all)   opts+=" --mail-type=ALL" ;;
      fail)  opts+=" --mail-type=FAIL" ;;
      end)   opts+=" --mail-type=END" ;;
      begin) opts+=" --mail-type=BEGIN" ;;
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

  local job_id
  job_id=$(sbatch --parsable $opts \
    -J "$job_name" \
    -o "$log_dir/${job_name}_%j.out" \
    -D "$(pwd)" \
    --export=ALL \
    "$script" "$@")

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

  # Submit with --wait flag
  local job_id
  job_id=$(sbatch --parsable --wait $opts \
    -J "$job_name" \
    -o "$log_dir/${job_name}_%j.out" \
    -D "$(pwd)" \
    --export=ALL \
    "$script" "$@")

  local exit_code=$?
  return $exit_code
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
    local job_id
    job_id=$(sbatch --parsable $opts \
      -J "$job_name" \
      -o "$log_dir/${job_name}_%j.out" \
      -D "$(pwd)" \
      --export=ALL \
      "$script" "$@" "$p")
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
      local job_id
      job_id=$(sbatch --parsable $opts \
        -J "$job_name" \
        -o "$log_dir/${job_name}_%j.out" \
        -D "$(pwd)" \
        --export=ALL \
        "$script" "$@" "$p1" "$p2")
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

    while true; do
      # Check job state using sacct
      local state
      state=$(sacct -j "$job_id" --format=State --noheader 2>/dev/null | head -1 | tr -d ' ')

      case "$state" in
        COMPLETED)
          break
          ;;
        FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED|OUT_OF_MEMORY)
          echo "Job $job_id ended with state: $state" >&2
          break
          ;;
        PENDING|RUNNING|COMPLETING|CONFIGURING|SUSPENDED)
          sleep 10
          ;;
        "")
          # Job might not be in sacct yet, check squeue
          if ! squeue -j "$job_id" -h >/dev/null 2>&1; then
            # Not in queue, check sacct again after a moment
            sleep 5
            state=$(sacct -j "$job_id" --format=State --noheader 2>/dev/null | head -1 | tr -d ' ')
            if [[ -z "$state" ]]; then
              echo "Warning: Cannot determine state for job $job_id" >&2
              break
            fi
          else
            sleep 10
          fi
          ;;
        *)
          echo "Unknown job state for $job_id: $state" >&2
          sleep 10
          ;;
      esac
    done
  done
}

# Get number of available CPUs in current job
function scheduler_get_slots() {
  echo "${SLURM_CPUS_PER_TASK:-${SLURM_NTASKS:-1}}"
}

# Check if running inside a SLURM job
function scheduler_in_job() {
  [[ -n "$SLURM_JOB_ID" ]]
}
