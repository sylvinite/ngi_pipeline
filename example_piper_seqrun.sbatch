#!/bin/bash

#SBATCH -A b2013064
#SBATCH -p node
#SBATCH -n 16
#SBATCH -t 4-00:00:00
#SBATCH -J Mario_seqrun_job
#SBATCH -o /path/to/stdout.out
#SBATCH -e /path/to/stderr.err

module load java/sun_jdk1.7.0_25
module load R/2.15.0

SCRATCH_ANALYSIS_DIR=whatever
SCRATCH_OUTPUT_DIR=whatever

# Copy files to the scratch drive
rsync -a project.analysis_dir /scratch/$SNIC_TMP

piper --align_and_qc whatever

EXIT_STATUS=$?

if [[ $(python check_charon_coverage(project="project", sample="sample", coverage=30)) ]]; then
    source activate ngi
    python $HOME/ngi_pipeline/scripts/start_pipeline_from_project \
            --sample-only $SCRATCH_ANALYSIS_DIR \
            --sample sample_name
    EXIT_STATUS=$?
fi

rsync -a /scratch/$SNIC_TMP project.analysis_dir
