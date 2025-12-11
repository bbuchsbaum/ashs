#!/usr/bin/env nextflow

/*
 * ASHS (Automatic Segmentation of Hippocampal Subfields)
 * Nextflow Pipeline Implementation
 *
 * This pipeline provides a modern, portable implementation of ASHS
 * that can run on any compute environment (local, SLURM, SGE, LSF, AWS, etc.)
 *
 * Usage:
 *   nextflow run main.nf --t1 /path/to/t1.nii.gz --t2 /path/to/t2.nii.gz --atlas /path/to/atlas
 *
 * With a sample sheet:
 *   nextflow run main.nf --input samples.csv --atlas /path/to/atlas
 */

nextflow.enable.dsl=2

// Pipeline version
version = '2.0.0'

// Default parameters
params.t1 = null
params.t2 = null
params.atlas = null
params.input = null
params.outdir = './results'
params.sides = 'left right'
params.help = false

// Resource defaults (can be overridden in config)
params.template_memory = '8.GB'
params.template_cpus = 4
params.multiatlas_memory = '16.GB'
params.multiatlas_cpus = 4
params.voting_memory = '8.GB'
params.voting_cpus = 2

// Show help message
def helpMessage() {
    log.info """
    =========================================
    ASHS - Automatic Segmentation of Hippocampal Subfields
    Version: ${version}
    =========================================

    Usage:
      nextflow run main.nf --t1 <t1.nii.gz> --t2 <t2.nii.gz> --atlas <atlas_dir>

    Required arguments:
      --t1            Path to T1-weighted MRI image
      --t2            Path to T2-weighted (TSE) MRI image
      --atlas         Path to ASHS atlas directory

    Optional arguments:
      --input         CSV file with columns: sample_id,t1,t2
      --outdir        Output directory (default: ./results)
      --sides         Sides to process: 'left right', 'left', or 'right' (default: 'left right')

    Resource configuration:
      --template_memory     Memory for template registration (default: 8.GB)
      --template_cpus       CPUs for template registration (default: 4)
      --multiatlas_memory   Memory for multi-atlas registration (default: 16.GB)
      --multiatlas_cpus     CPUs for multi-atlas registration (default: 4)

    Profiles:
      -profile local        Run on local machine
      -profile slurm        Run on SLURM cluster
      -profile sge          Run on SGE cluster
      -profile lsf          Run on LSF cluster
      -profile singularity  Use Singularity container
      -profile docker       Use Docker container

    Examples:
      # Single subject
      nextflow run main.nf --t1 sub01_t1.nii.gz --t2 sub01_t2.nii.gz --atlas /data/ashs_atlas

      # Multiple subjects via sample sheet
      nextflow run main.nf --input samples.csv --atlas /data/ashs_atlas -profile slurm

      # With Singularity container
      nextflow run main.nf --input samples.csv --atlas /data/ashs_atlas -profile slurm,singularity
    """.stripIndent()
}

// Show help if requested
if (params.help) {
    helpMessage()
    exit 0
}

// Validate required parameters
if (!params.atlas) {
    log.error "ERROR: --atlas parameter is required"
    helpMessage()
    exit 1
}

// Log pipeline info
log.info """
=========================================
ASHS Pipeline v${version}
=========================================
atlas    : ${params.atlas}
outdir   : ${params.outdir}
sides    : ${params.sides}
-----------------------------------------
"""

/*
 * Channel creation
 */

// Create input channel from either single subject or sample sheet
if (params.input) {
    // Sample sheet mode: CSV with sample_id,t1,t2
    Channel
        .fromPath(params.input)
        .splitCsv(header: true)
        .map { row -> tuple(row.sample_id, file(row.t1), file(row.t2)) }
        .set { subjects_ch }
} else if (params.t1 && params.t2) {
    // Single subject mode
    Channel
        .of(tuple('subject', file(params.t1), file(params.t2)))
        .set { subjects_ch }
} else {
    log.error "ERROR: Either --input (sample sheet) or --t1 and --t2 must be provided"
    helpMessage()
    exit 1
}

// Create sides channel
Channel
    .of(params.sides.tokenize(' '))
    .flatten()
    .set { sides_ch }

// Atlas channel
atlas_ch = file(params.atlas)

/*
 * Process Definitions
 */

/*
 * Stage 1: Template Registration
 * Register subject T1 to atlas template space
 */
process TEMPLATE_REGISTRATION {
    tag "${sample_id}"
    label 'process_medium'

    memory params.template_memory
    cpus params.template_cpus

    publishDir "${params.outdir}/${sample_id}/template", mode: 'copy'

    input:
    tuple val(sample_id), path(t1), path(t2)
    path atlas

    output:
    tuple val(sample_id), path(t1), path(t2), path("template_to_subject*.mat"), path("subject_to_template*.nii.gz"), emit: registered
    path "*.log", emit: logs

    script:
    """
    # Source ASHS environment
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run template registration
    bash \$ASHS_ROOT/bin/ashs_template_qsub.sh ${t1} ${t2} > template_registration.log 2>&1

    # Or use greedy directly for more control:
    # greedy -d 3 -a -m NCC 2x2x2 \\
    #   -i ${atlas}/template/template.nii.gz ${t1} \\
    #   -o template_to_subject_affine.mat \\
    #   -n 100x50x10 -threads ${task.cpus}
    """
}

/*
 * Stage 2: Multi-Atlas Registration
 * Register each atlas to the subject for each side
 */
process MULTIATLAS_REGISTRATION {
    tag "${sample_id}_${side}_${atlas_id}"
    label 'process_high'

    memory params.multiatlas_memory
    cpus params.multiatlas_cpus

    publishDir "${params.outdir}/${sample_id}/multiatlas/${side}", mode: 'copy'

    input:
    tuple val(sample_id), path(t1), path(t2), path(template_mat), path(template_warp)
    each side
    each atlas_id
    path atlas

    output:
    tuple val(sample_id), val(side), val(atlas_id), path("${side}_${atlas_id}_seg.nii.gz"), emit: segmentations
    path "*.log", emit: logs

    script:
    """
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run multi-atlas registration for this atlas and side
    bash \$ASHS_ROOT/bin/ashs_multiatlas_qsub.sh ${side} ${atlas_id} > multiatlas_${side}_${atlas_id}.log 2>&1
    """
}

/*
 * Stage 3: Initial Voting / Label Fusion
 * Combine multi-atlas segmentations using joint label fusion
 */
process VOTING_INITIAL {
    tag "${sample_id}_${side}"
    label 'process_medium'

    memory params.voting_memory
    cpus params.voting_cpus

    publishDir "${params.outdir}/${sample_id}/voting/${side}", mode: 'copy'

    input:
    tuple val(sample_id), val(side), path(segmentations)
    path atlas

    output:
    tuple val(sample_id), val(side), path("${side}_consensus_init.nii.gz"), emit: consensus
    path "*.log", emit: logs

    script:
    """
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run voting for initial segmentation
    bash \$ASHS_ROOT/bin/ashs_voting_qsub.sh ${side} 0 > voting_${side}_init.log 2>&1
    """
}

/*
 * Stage 4: Bootstrap Registration
 * Refine registrations using initial consensus
 */
process BOOTSTRAP_REGISTRATION {
    tag "${sample_id}_${side}_${atlas_id}"
    label 'process_high'

    memory params.multiatlas_memory
    cpus params.multiatlas_cpus

    publishDir "${params.outdir}/${sample_id}/bootstrap/${side}", mode: 'copy'

    input:
    tuple val(sample_id), val(side), path(consensus)
    each atlas_id
    path atlas

    output:
    tuple val(sample_id), val(side), val(atlas_id), path("${side}_${atlas_id}_bootstrap_seg.nii.gz"), emit: segmentations
    path "*.log", emit: logs

    script:
    """
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run bootstrap registration
    bash \$ASHS_ROOT/bin/ashs_bootstrap_qsub.sh ${side} ${atlas_id} > bootstrap_${side}_${atlas_id}.log 2>&1
    """
}

/*
 * Stage 5: Final Voting
 * Combine bootstrap segmentations
 */
process VOTING_FINAL {
    tag "${sample_id}_${side}"
    label 'process_medium'

    memory params.voting_memory
    cpus params.voting_cpus

    publishDir "${params.outdir}/${sample_id}/final/${side}", mode: 'copy'

    input:
    tuple val(sample_id), val(side), path(bootstrap_segs)
    path atlas

    output:
    tuple val(sample_id), val(side), path("${side}_lfseg_heur.nii.gz"), path("${side}_lfseg_corr_usegray.nii.gz"), emit: final_seg
    path "*.log", emit: logs

    script:
    """
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run final voting with error correction
    bash \$ASHS_ROOT/bin/ashs_voting_qsub.sh ${side} 1 > voting_${side}_final.log 2>&1
    """
}

/*
 * Stage 6: Quality Assurance
 * Generate QA images and reports
 */
process QUALITY_ASSURANCE {
    tag "${sample_id}"
    label 'process_low'

    publishDir "${params.outdir}/${sample_id}/qa", mode: 'copy'

    input:
    tuple val(sample_id), path(final_segs)
    path atlas

    output:
    tuple val(sample_id), path("qa_*.png"), path("qa_report.html"), emit: qa_outputs
    path "*.log", emit: logs

    script:
    """
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run QA
    bash \$ASHS_ROOT/bin/ashs_finalqa_qsub.sh > qa.log 2>&1
    """
}

/*
 * Stage 7: Statistics Extraction
 * Extract volumes and other statistics
 */
process EXTRACT_STATISTICS {
    tag "${sample_id}"
    label 'process_low'

    publishDir "${params.outdir}/${sample_id}/stats", mode: 'copy'

    input:
    tuple val(sample_id), path(final_segs)
    path atlas

    output:
    tuple val(sample_id), path("${sample_id}_volumes.txt"), path("${sample_id}_stats.csv"), emit: statistics
    path "*.log", emit: logs

    script:
    """
    export ASHS_ROOT=\${ASHS_ROOT:-/opt/ashs}
    export ASHS_WORK=\$(pwd)
    export ASHS_ATLAS=${atlas}

    # Run statistics extraction
    bash \$ASHS_ROOT/bin/ashs_extractstats_qsub.sh > stats.log 2>&1

    # Rename outputs with sample ID
    mv volumes.txt ${sample_id}_volumes.txt 2>/dev/null || true
    mv stats.csv ${sample_id}_stats.csv 2>/dev/null || true
    """
}

/*
 * Aggregate statistics across all subjects
 */
process AGGREGATE_STATISTICS {
    tag "aggregate"
    label 'process_low'

    publishDir "${params.outdir}/aggregate", mode: 'copy'

    input:
    path stats_files

    output:
    path "all_subjects_volumes.csv"
    path "summary_statistics.csv"

    script:
    """
    #!/usr/bin/env python3
    import pandas as pd
    import glob

    # Combine all individual stats files
    all_stats = []
    for f in glob.glob("*_stats.csv"):
        df = pd.read_csv(f)
        all_stats.append(df)

    if all_stats:
        combined = pd.concat(all_stats, ignore_index=True)
        combined.to_csv("all_subjects_volumes.csv", index=False)

        # Generate summary statistics
        summary = combined.describe()
        summary.to_csv("summary_statistics.csv")
    else:
        # Create empty files if no stats
        open("all_subjects_volumes.csv", 'w').write("No data\\n")
        open("summary_statistics.csv", 'w').write("No data\\n")
    """
}

/*
 * Main Workflow
 */
workflow {
    // Get list of atlas IDs from atlas directory
    atlas_ids_ch = Channel
        .fromPath("${params.atlas}/train/train_identity.txt")
        .splitText()
        .map { it.trim() }
        .filter { it }

    // Stage 1: Template registration
    TEMPLATE_REGISTRATION(subjects_ch, atlas_ch)

    // Combine registered subjects with sides
    registered_with_sides = TEMPLATE_REGISTRATION.out.registered
        .combine(sides_ch)

    // Stage 2: Multi-atlas registration
    // This creates all (subject, side, atlas) combinations
    MULTIATLAS_REGISTRATION(
        TEMPLATE_REGISTRATION.out.registered,
        sides_ch,
        atlas_ids_ch,
        atlas_ch
    )

    // Group multi-atlas results by (sample_id, side)
    multiatlas_grouped = MULTIATLAS_REGISTRATION.out.segmentations
        .groupTuple(by: [0, 1])
        .map { sample_id, side, atlas_ids, segs -> tuple(sample_id, side, segs) }

    // Stage 3: Initial voting
    VOTING_INITIAL(multiatlas_grouped, atlas_ch)

    // Stage 4: Bootstrap registration
    BOOTSTRAP_REGISTRATION(
        VOTING_INITIAL.out.consensus,
        atlas_ids_ch,
        atlas_ch
    )

    // Group bootstrap results by (sample_id, side)
    bootstrap_grouped = BOOTSTRAP_REGISTRATION.out.segmentations
        .groupTuple(by: [0, 1])
        .map { sample_id, side, atlas_ids, segs -> tuple(sample_id, side, segs) }

    // Stage 5: Final voting
    VOTING_FINAL(bootstrap_grouped, atlas_ch)

    // Group final results by sample_id
    final_by_sample = VOTING_FINAL.out.final_seg
        .groupTuple(by: 0)

    // Stage 6: QA
    QUALITY_ASSURANCE(final_by_sample, atlas_ch)

    // Stage 7: Statistics
    EXTRACT_STATISTICS(final_by_sample, atlas_ch)

    // Aggregate all statistics
    all_stats = EXTRACT_STATISTICS.out.statistics
        .map { sample_id, volumes, stats -> stats }
        .collect()

    AGGREGATE_STATISTICS(all_stats)
}

/*
 * Completion handler
 */
workflow.onComplete {
    log.info """
    =========================================
    Pipeline completed!
    =========================================
    Status:     ${workflow.success ? 'SUCCESS' : 'FAILED'}
    Duration:   ${workflow.duration}
    Output:     ${params.outdir}
    -----------------------------------------
    """
}
