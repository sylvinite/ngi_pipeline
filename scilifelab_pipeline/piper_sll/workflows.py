#!/bin/env python
"""Piper workflow-specific code."""


from scilifelab_pipeline.log import minimal_logger
from scilifelab_pipeline.utils import load_yaml_config_expand_vars


LOG = minimal_logger(__name__)

def return_cl_for_workflow(workflow_name, setup_xml_path, piper_globalconfig):
    """Return an executable-ready Piper command line.

    :param str workflow_name: The name of the Piper workflow to be run.
    :param dict piper_globalconfig: The parsed Piper-specific globalConfig file.

    :returns: The Piper command line to be executed.
    :rtype: str
    :raises OSError: If the configuration file cannot be read.
    :raises ValueError: If a required configuration value is missing.
    """
    workflow_fn_name = "workflow_{}".format(workflow_name)
    # Get the local function if it exists
    try:
        workflow_function = sys.modules[__name__].workflow_fn_name
    except AttributeError as e:
        error_msg = "Workflow \"{}\" has no associated function.".format(workflow_fn_name)
        LOG.error(error_msg)
        raise NotImplementedError(error_msg)
    ## TODO add module loading of:
    ##          java/sun_jdk1.7.0_25
    ##          R/2.15.0
    ##      to command line, or deal with this in the calling function.
    return workflow_function(setup_xml_path, piper_globalconfig)


def workflow_dna_alignonly(*args, **kwargs):
    """Return the command line for basic DNA Alignment.

    :param strs path_to_qscripts: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config: The parsed Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    # Same command line but with one additional option
    return workflow_dna_variantcalling(*args, **kwargs) + " --alignment_and_qc"


def workflow_dna_variantcalling(path_to_qscripts, setup_xml_path, global_config):
    """Return the command line for DNA Variant Calling.

    :param strs path_to_qscripts: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config: The parsed Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    ## TODO Should we check for the existence of this file up front or let the error hit later in Piper?
    qscript_path = os.path.join(path_to_qscripts, "DNABestPracticeVariantCalling.scala")
    global_config = utils.lowercase_keys(global_config)

    ## Just for reference, delete later
            #piper -S ${SCRIPTS_DIR}/DNABestPracticeVariantCalling.scala \
            #--xml_input ${PIPELINE_SETUP} \
            #--dbsnp ${DB_SNP_B37} \
            #--extra_indels ${MILLS_B37} \
            #--extra_indels ${ONE_K_G_B37} \
            #--hapmap ${HAPMAP_B37} \
            #--omni ${OMNI_B37} \
            #--mills ${MILLS_B37} \
            #--thousandGenomes ${THOUSAND_GENOMES_B37} \
            #-bwa ${PATH_TO_BWA} \
            #-samtools ${PATH_TO_SAMTOOLS} \
            #-qualimap ${PATH_TO_QUALIMAP} \
            #--number_of_threads 8 \
            #--scatter_gather 23 \
            #-jobRunner ${JOB_RUNNER} \
            #-jobNative "${JOB_NATIVE_ARGS}" \
            #--job_walltime 345600 \
            #${RUN} ${ONLY_ALIGMENTS} ${DEBUG} 2>&1 | tee -a ${LOGS}/wholeGenome.log

    return  "piper -S {path_to_qscripts} " \
            "--xml_input {setup_xml_path} " \
            "--dbsnp {db_snp_b37} " \
            "--extra_indels {mills_b37} " \
            "--extra_indels {one_k_g_b37} " \
            "--hapmap {hapmap_b37} " \
            "--omni {omni_b37} " \
            "--thousandGenomes {thousand_genomes_b37} " \
            "-bwa {path_to_bwa} " \
            "-samtools {path_to_samtools} " \
            "-qualimap {path_to_qualimap} " \
            "--number_of_threads {num_threads} "\
            "--scatter_gather 23 " \
            "-jobRunner {job_runner} " \
            "-jobNative \"{job_native_args}\" "\
            "--job_walltime {job_walltime} " \
            "-run".format(**global_config)

