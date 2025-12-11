#!/bin/bash
#######################################################################
#
#  Program:   ASHS (Automatic Segmentation of Hippocampal Subfields)
#  Module:    Scheduler Library
#  Language:  BASH Shell Script
#
#  This file is part of ASHS
#
#  ASHS is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#######################################################################

# ============================================================================
# ASHS Scheduler Abstraction Library
# ============================================================================
#
# This library provides a unified interface for job scheduling across
# different batch systems (SLURM, SGE, LSF, GNU Parallel, local).
#
# Usage:
#   source ashs_scheduler_lib.sh
#   scheduler_init                    # Detect/configure scheduler
#   scheduler_submit "job.sh" $STAGE  # Submit a job
#   scheduler_wait $JOB_ID            # Wait for job completion
#
# ============================================================================

# Global variables set by this library
ASHS_DETECTED_SCHEDULER=""
ASHS_SCHEDULER_AVAILABLE=0

# ----------------------------------------------------------------------------
# Load Configuration
# ----------------------------------------------------------------------------
# Search for configuration in order of priority:
#   1. $ASHS_SCHEDULER_CONF (if set)
#   2. ./ashs_scheduler.conf (current directory)
#   3. $ASHS_WORK/ashs_scheduler.conf (working directory)
#   4. ~/.ashs_scheduler.conf (user home)
#   5. $ASHS_ROOT/bin/ashs_scheduler.conf (default)

function scheduler_load_config() {
  local config_file=""

  if [[ -n "$ASHS_SCHEDULER_CONF" && -f "$ASHS_SCHEDULER_CONF" ]]; then
    config_file="$ASHS_SCHEDULER_CONF"
  elif [[ -f "./ashs_scheduler.conf" ]]; then
    config_file="./ashs_scheduler.conf"
  elif [[ -n "$ASHS_WORK" && -f "$ASHS_WORK/ashs_scheduler.conf" ]]; then
    config_file="$ASHS_WORK/ashs_scheduler.conf"
  elif [[ -f "$HOME/.ashs_scheduler.conf" ]]; then
    config_file="$HOME/.ashs_scheduler.conf"
  elif [[ -n "$ASHS_ROOT" && -f "$ASHS_ROOT/bin/ashs_scheduler.conf" ]]; then
    config_file="$ASHS_ROOT/bin/ashs_scheduler.conf"
  fi

  if [[ -n "$config_file" ]]; then
    source "$config_file"
    echo "Loaded scheduler config from: $config_file"
  fi
}

# ----------------------------------------------------------------------------
# Scheduler Detection
# ----------------------------------------------------------------------------

function scheduler_detect_slurm() {
  command -v sbatch >/dev/null 2>&1 && command -v squeue >/dev/null 2>&1
}

function scheduler_detect_sge() {
  [[ -n "$SGE_ROOT" ]] && command -v qsub >/dev/null 2>&1
}

function scheduler_detect_lsf() {
  [[ -n "$LSF_BINDIR" ]] && command -v bsub >/dev/null 2>&1
}

function scheduler_detect_parallel() {
  command -v parallel >/dev/null 2>&1
}

function scheduler_detect() {
  # Check priority order from config, or use default
  local priority="${ASHS_SCHEDULER_PRIORITY:-slurm sge lsf parallel local}"

  for sched in $priority; do
    case $sched in
      slurm)
        if scheduler_detect_slurm; then
          ASHS_DETECTED_SCHEDULER="slurm"
          return 0
        fi
        ;;
      sge)
        if scheduler_detect_sge; then
          ASHS_DETECTED_SCHEDULER="sge"
          return 0
        fi
        ;;
      lsf)
        if scheduler_detect_lsf; then
          ASHS_DETECTED_SCHEDULER="lsf"
          return 0
        fi
        ;;
      parallel)
        if scheduler_detect_parallel; then
          ASHS_DETECTED_SCHEDULER="parallel"
          return 0
        fi
        ;;
      local)
        ASHS_DETECTED_SCHEDULER="local"
        return 0
        ;;
    esac
  done

  # Fallback to local
  ASHS_DETECTED_SCHEDULER="local"
  return 0
}

# ----------------------------------------------------------------------------
# Initialize Scheduler
# ----------------------------------------------------------------------------

function scheduler_init() {
  # Load configuration first
  scheduler_load_config

  # Determine which scheduler to use
  local requested="${ASHS_SCHEDULER:-auto}"

  # Handle legacy environment variables
  if [[ -n "$ASHS_USE_SLURM" ]]; then
    requested="slurm"
  elif [[ -n "$ASHS_USE_QSUB" ]]; then
    requested="sge"
  elif [[ -n "$ASHS_USE_LSF" ]]; then
    requested="lsf"
  elif [[ -n "$ASHS_USE_PARALLEL" ]]; then
    requested="parallel"
  fi

  if [[ "$requested" == "auto" ]]; then
    scheduler_detect
  else
    ASHS_DETECTED_SCHEDULER="$requested"
  fi

  # Validate the scheduler is available
  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      if ! scheduler_detect_slurm; then
        echo "ERROR: SLURM requested but sbatch/squeue not found"
        return 1
      fi
      ;;
    sge)
      if ! scheduler_detect_sge; then
        echo "ERROR: SGE requested but SGE_ROOT not set or qsub not found"
        return 1
      fi
      ;;
    lsf)
      if ! scheduler_detect_lsf; then
        echo "ERROR: LSF requested but LSF_BINDIR not set or bsub not found"
        return 1
      fi
      ;;
    parallel)
      if ! scheduler_detect_parallel; then
        echo "ERROR: GNU Parallel requested but 'parallel' not found"
        return 1
      fi
      ;;
  esac

  ASHS_SCHEDULER_AVAILABLE=1
  echo "Using scheduler: $ASHS_DETECTED_SCHEDULER"
  return 0
}

# ----------------------------------------------------------------------------
# Build Scheduler Options for a Stage
# ----------------------------------------------------------------------------

function scheduler_build_opts() {
  local stage="${1:-0}"
  local opts=""

  # Get stage-specific or default values
  local mem_var="ASHS_STAGE_${stage}_MEMORY"
  local cores_var="ASHS_STAGE_${stage}_CORES"
  local time_var="ASHS_STAGE_${stage}_TIME"

  local memory="${!mem_var:-$ASHS_DEFAULT_MEMORY}"
  local cores="${!cores_var:-$ASHS_DEFAULT_CORES}"
  local walltime="${!time_var:-$ASHS_DEFAULT_TIME}"
  local queue="${ASHS_DEFAULT_QUEUE}"
  local email="${ASHS_NOTIFY_EMAIL}"
  local events="${ASHS_NOTIFY_EVENTS:-fail}"

  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      [[ -n "$memory" ]] && opts+=" --mem=$memory"
      [[ -n "$cores" ]] && opts+=" --cpus-per-task=$cores"
      [[ -n "$walltime" ]] && opts+=" --time=$walltime"
      [[ -n "$queue" ]] && opts+=" --partition=$queue"
      [[ -n "$email" ]] && opts+=" --mail-user=$email"
      if [[ -n "$email" ]]; then
        case $events in
          all) opts+=" --mail-type=ALL" ;;
          fail) opts+=" --mail-type=FAIL" ;;
          end) opts+=" --mail-type=END" ;;
          begin) opts+=" --mail-type=BEGIN" ;;
        esac
      fi
      opts+=" ${ASHS_SLURM_EXTRA_OPTS}"
      ;;

    sge)
      [[ -n "$memory" ]] && opts+=" -l h_vmem=$memory"
      [[ -n "$cores" ]] && opts+=" -pe smp $cores"
      [[ -n "$walltime" ]] && opts+=" -l h_rt=$walltime"
      [[ -n "$queue" ]] && opts+=" -q $queue"
      [[ -n "$email" ]] && opts+=" -M $email"
      if [[ -n "$email" ]]; then
        case $events in
          all) opts+=" -m beas" ;;
          fail) opts+=" -m a" ;;
          end) opts+=" -m e" ;;
          begin) opts+=" -m b" ;;
        esac
      fi
      opts+=" ${ASHS_SGE_EXTRA_OPTS}"
      ;;

    lsf)
      [[ -n "$memory" ]] && opts+=" -R 'rusage[mem=${memory%G}000]'"
      [[ -n "$cores" ]] && opts+=" -n $cores"
      [[ -n "$walltime" ]] && opts+=" -W ${walltime%:*}"
      [[ -n "$queue" ]] && opts+=" -q $queue"
      [[ -n "$email" ]] && opts+=" -u $email"
      if [[ -n "$email" ]]; then
        case $events in
          all) opts+=" -N" ;;
          fail) opts+=" -N" ;;
          end) opts+=" -N" ;;
        esac
      fi
      opts+=" ${ASHS_LSF_EXTRA_OPTS}"
      ;;

    parallel)
      [[ -n "$cores" ]] && opts+=" -j $cores"
      opts+=" ${ASHS_PARALLEL_EXTRA_OPTS}"
      ;;

    local)
      # No options for local execution
      ;;
  esac

  echo "$opts"
}

# ----------------------------------------------------------------------------
# Job Submission Functions
# ----------------------------------------------------------------------------

# Submit a single job and return the job ID
function scheduler_submit() {
  local script="$1"
  local stage="${2:-0}"
  local name="${3:-ashs_job}"
  local opts
  opts=$(scheduler_build_opts "$stage")

  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  mkdir -p "$log_dir"

  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      local job_id
      job_id=$(sbatch --parsable $opts \
        -J "${ASHS_JOB_PREFIX:-ashs}_${name}" \
        -o "$log_dir/${name}_%j.out" \
        -e "$log_dir/${name}_%j.err" \
        "$script" "${@:4}")
      echo "$job_id"
      ;;

    sge)
      local job_id
      job_id=$(qsub $opts \
        -N "${ASHS_JOB_PREFIX:-ashs}_${name}" \
        -o "$log_dir" \
        -e "$log_dir" \
        -cwd -V \
        "$script" "${@:4}" | awk '{print $3}')
      echo "$job_id"
      ;;

    lsf)
      local job_id
      job_id=$(bsub $opts \
        -J "${ASHS_JOB_PREFIX:-ashs}_${name}" \
        -o "$log_dir/${name}_%J.out" \
        -e "$log_dir/${name}_%J.err" \
        -cwd "$PWD" \
        "$script" "${@:4}" | awk -F'[<>]' '{print $2}')
      echo "$job_id"
      ;;

    parallel)
      # GNU Parallel doesn't have job IDs in the same way
      # Execute in background and return PID
      bash "$script" "${@:4}" &
      echo $!
      ;;

    local)
      # Execute directly (blocking)
      bash "$script" "${@:4}"
      echo "0"
      ;;
  esac
}

# Submit a job and wait for completion
function scheduler_submit_sync() {
  local script="$1"
  local stage="${2:-0}"
  local name="${3:-ashs_job}"
  local opts
  opts=$(scheduler_build_opts "$stage")

  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  mkdir -p "$log_dir"

  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      sbatch --wait $opts \
        -J "${ASHS_JOB_PREFIX:-ashs}_${name}" \
        -o "$log_dir/${name}_%j.out" \
        -e "$log_dir/${name}_%j.err" \
        "$script" "${@:4}"
      ;;

    sge)
      qsub -sync y $opts \
        -N "${ASHS_JOB_PREFIX:-ashs}_${name}" \
        -o "$log_dir" \
        -e "$log_dir" \
        -cwd -V \
        "$script" "${@:4}"
      ;;

    lsf)
      bsub -K $opts \
        -J "${ASHS_JOB_PREFIX:-ashs}_${name}" \
        -o "$log_dir/${name}_%J.out" \
        -e "$log_dir/${name}_%J.err" \
        -cwd "$PWD" \
        "$script" "${@:4}"
      ;;

    parallel|local)
      # Execute directly (blocking)
      bash "$script" "${@:4}"
      ;;
  esac
}

# Submit array of jobs with single parameter
function scheduler_submit_array() {
  local script="$1"
  local params="$2"     # Space-separated list
  local stage="${3:-0}"
  local name="${4:-ashs_array}"
  local opts
  opts=$(scheduler_build_opts "$stage")

  local log_dir="${ASHS_WORK:-$(pwd)}/dump"
  mkdir -p "$log_dir"

  local job_ids=()

  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      for p in $params; do
        local job_id
        job_id=$(sbatch --parsable $opts \
          -J "${ASHS_JOB_PREFIX:-ashs}_${name}_${p}" \
          -o "$log_dir/${name}_${p}_%j.out" \
          -e "$log_dir/${name}_${p}_%j.err" \
          "$script" "$p")
        job_ids+=("$job_id")
      done
      ;;

    sge)
      for p in $params; do
        local job_id
        job_id=$(qsub $opts \
          -N "${ASHS_JOB_PREFIX:-ashs}_${name}_${p}" \
          -o "$log_dir" \
          -e "$log_dir" \
          -cwd -V \
          "$script" "$p" | awk '{print $3}')
        job_ids+=("$job_id")
      done
      ;;

    lsf)
      for p in $params; do
        local job_id
        job_id=$(bsub $opts \
          -J "${ASHS_JOB_PREFIX:-ashs}_${name}_${p}" \
          -o "$log_dir/${name}_${p}_%J.out" \
          -e "$log_dir/${name}_${p}_%J.err" \
          -cwd "$PWD" \
          "$script" "$p" | awk -F'[<>]' '{print $2}')
        job_ids+=("$job_id")
      done
      ;;

    parallel)
      parallel $opts bash "$script" {} ::: $params &
      job_ids+=($!)
      ;;

    local)
      for p in $params; do
        bash "$script" "$p"
      done
      ;;
  esac

  echo "${job_ids[*]}"
}

# Wait for job(s) to complete
function scheduler_wait() {
  local job_ids="$1"

  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      for job_id in $job_ids; do
        while true; do
          local state
          state=$(sacct -j "$job_id" --format=State --noheader 2>/dev/null | head -1 | tr -d ' ')
          case $state in
            COMPLETED|FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED|OUT_OF_MEMORY)
              break
              ;;
            *)
              sleep 10
              ;;
          esac
        done
      done
      ;;

    sge)
      # Use qsub with hold_jid to wait
      for job_id in $job_ids; do
        qsub -b y -sync y -hold_jid "$job_id" -o /dev/null /bin/sleep 1 2>/dev/null
      done
      ;;

    lsf)
      for job_id in $job_ids; do
        bsub -K -w "ended($job_id)" -o /dev/null /bin/sleep 1 2>/dev/null
      done
      ;;

    parallel|local)
      for pid in $job_ids; do
        wait "$pid" 2>/dev/null
      done
      ;;
  esac
}

# ----------------------------------------------------------------------------
# Utility Functions
# ----------------------------------------------------------------------------

# Get scheduler info for display
function scheduler_info() {
  echo "Scheduler: $ASHS_DETECTED_SCHEDULER"
  echo "Available: $ASHS_SCHEDULER_AVAILABLE"

  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      echo "Version: $(sbatch --version 2>/dev/null || echo 'unknown')"
      ;;
    sge)
      echo "SGE_ROOT: $SGE_ROOT"
      ;;
    lsf)
      echo "LSF_BINDIR: $LSF_BINDIR"
      ;;
    parallel)
      echo "Version: $(parallel --version 2>/dev/null | head -1 || echo 'unknown')"
      ;;
  esac
}

# Check if we're running inside a scheduled job
function scheduler_in_job() {
  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      [[ -n "$SLURM_JOB_ID" ]]
      ;;
    sge)
      [[ -n "$JOB_ID" ]]
      ;;
    lsf)
      [[ -n "$LSB_JOBID" ]]
      ;;
    *)
      return 1
      ;;
  esac
}

# Get number of available slots/cores
function scheduler_get_slots() {
  case $ASHS_DETECTED_SCHEDULER in
    slurm)
      echo "${SLURM_CPUS_PER_TASK:-1}"
      ;;
    sge)
      echo "${NSLOTS:-1}"
      ;;
    lsf)
      echo "${LSB_MAX_NUM_PROCESSORS:-1}"
      ;;
    parallel|local)
      nproc 2>/dev/null || echo "1"
      ;;
  esac
}
