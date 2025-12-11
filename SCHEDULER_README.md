# ASHS Scheduler Configuration Guide

This document explains the new modular scheduler system in ASHS, which provides
flexible support for different batch queuing systems.

## Quick Start

### Using Configuration File (Recommended)

1. Copy the default configuration:
   ```bash
   cp $ASHS_ROOT/bin/ashs_scheduler.conf ~/.ashs_scheduler.conf
   ```

2. Edit `~/.ashs_scheduler.conf` to set your preferred scheduler and resources:
   ```bash
   ASHS_SCHEDULER="slurm"
   ASHS_DEFAULT_MEMORY="8G"
   ASHS_DEFAULT_CORES="4"
   ASHS_DEFAULT_QUEUE="normal"
   ```

3. Run ASHS as usual:
   ```bash
   ashs_main.sh -a atlas -g t1.nii.gz -f t2.nii.gz -w output
   ```

### Using Command-Line Flags (Legacy)

The traditional command-line flags still work:

```bash
# SLURM
ashs_main.sh -S -a atlas -g t1.nii.gz -f t2.nii.gz -w output

# SGE
ashs_main.sh -Q -a atlas -g t1.nii.gz -f t2.nii.gz -w output

# LSF
ashs_main.sh -l -a atlas -g t1.nii.gz -f t2.nii.gz -w output

# GNU Parallel (local multi-core)
ashs_main.sh -P -a atlas -g t1.nii.gz -f t2.nii.gz -w output
```

## Supported Schedulers

| Scheduler | Config Value | CLI Flag | Requirements |
|-----------|--------------|----------|--------------|
| SLURM | `slurm` | `-S` | `sbatch`, `squeue`, `sacct` in PATH |
| SGE | `sge` | `-Q` | `SGE_ROOT` set, `qsub` in PATH |
| LSF | `lsf` | `-l` | `LSF_BINDIR` set, `bsub` in PATH |
| GNU Parallel | `parallel` | `-P` | `parallel` in PATH |
| Local (sequential) | `local` | (none) | Always available |
| Auto-detect | `auto` | (none) | Default behavior |

## Configuration File

The configuration file is searched in this order:
1. `$ASHS_SCHEDULER_CONF` (if set)
2. `./ashs_scheduler.conf` (current directory)
3. `$ASHS_WORK/ashs_scheduler.conf` (working directory)
4. `~/.ashs_scheduler.conf` (home directory)
5. `$ASHS_ROOT/bin/ashs_scheduler.conf` (default)

### Configuration Options

```bash
# Scheduler selection
ASHS_SCHEDULER="auto"           # auto, slurm, sge, lsf, parallel, local

# Default resources
ASHS_DEFAULT_MEMORY="8G"        # Memory per job
ASHS_DEFAULT_CORES="4"          # CPUs per job
ASHS_DEFAULT_TIME="4:00:00"     # Wall time
ASHS_DEFAULT_QUEUE="normal"     # Queue/partition name

# Per-stage overrides (stages 1-7)
ASHS_STAGE_2_MEMORY="16G"       # Multi-atlas needs more memory
ASHS_STAGE_4_MEMORY="16G"       # Bootstrap also memory-intensive

# Scheduler-specific extras
ASHS_SLURM_EXTRA_OPTS="--account=myaccount"
ASHS_SGE_EXTRA_OPTS="-l scratch=10G"
ASHS_LSF_EXTRA_OPTS="-R 'span[hosts=1]'"

# Notifications
ASHS_NOTIFY_EMAIL="user@example.com"
ASHS_NOTIFY_EVENTS="fail"       # all, fail, end, begin, none
```

## Scheduler Plugins

The new architecture uses modular scheduler adapters located in:
```
$ASHS_ROOT/bin/schedulers/
├── slurm.sh
├── sge.sh
├── lsf.sh
├── parallel.sh
└── local.sh
```

Each adapter implements a standard API:
- `scheduler_detect()` - Check if scheduler is available
- `scheduler_submit()` - Submit a job
- `scheduler_submit_sync()` - Submit and wait
- `scheduler_submit_array_single()` - Submit array with one parameter
- `scheduler_submit_array_double()` - Submit array with two parameters
- `scheduler_wait()` - Wait for job completion

### Adding a New Scheduler

To add support for a new scheduler (e.g., PBS):

1. Create `$ASHS_ROOT/bin/schedulers/pbs.sh`
2. Implement the required functions
3. Add `pbs` to `ASHS_SCHEDULER_PRIORITY` in the config

## Using Containers

### Singularity (Recommended for HPC)

```bash
# Build container
singularity build ashs.sif Singularity.def

# Run with SLURM
singularity exec ashs.sif ashs_main.sh -S -a atlas -g t1.nii.gz -f t2.nii.gz -w output

# Or run via scheduler submission
sbatch --wrap="singularity exec ashs.sif ashs_main.sh ..."
```

### Docker

```bash
# Build container
docker build -t ashs:latest .

# Run
docker run -v /data:/data ashs:latest -a /data/atlas -g /data/t1.nii.gz -f /data/t2.nii.gz -w /data/output
```

## Nextflow Pipeline (Recommended for Production)

For production use, we recommend the Nextflow pipeline which provides:
- Automatic scheduler detection
- Built-in error recovery and resume
- Cloud support (AWS, Google Cloud)
- Detailed execution reports

```bash
# Install Nextflow
curl -s https://get.nextflow.io | bash

# Run on SLURM with Singularity
nextflow run nextflow/main.nf \
  --input samples.csv \
  --atlas /path/to/atlas \
  -profile slurm,singularity

# Run locally
nextflow run nextflow/main.nf \
  --t1 subject_t1.nii.gz \
  --t2 subject_t2.nii.gz \
  --atlas /path/to/atlas \
  -profile local
```

See `nextflow/` directory for full documentation.

## Troubleshooting

### Scheduler not detected
```
ERROR: SLURM requested but sbatch/squeue not found
```
Ensure the scheduler binaries are in your PATH and any required environment
variables (SGE_ROOT, LSF_BINDIR) are set.

### Jobs fail immediately
Check the log files in `$ASHS_WORK/dump/` for error messages.

### Memory errors
Increase memory allocation:
```bash
ASHS_DEFAULT_MEMORY="16G"
# Or for specific stages:
ASHS_STAGE_2_MEMORY="32G"
```

### Jobs stuck in queue
- Check queue status with your scheduler's tools
- Verify queue name is correct
- Check account/allocation settings
