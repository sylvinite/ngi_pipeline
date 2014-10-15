"""QC workflow-specific code."""

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config

LOG = minimal_logger(__name__)


@with_ngi_config
def return_cl_for_workflow(workflow_name, config=None, config_file_path=None):
    """Return an executable-ready bash command line.

    :param str workflow_name: The name of the workflow to be run.
    """


def workflow_fastq_screen(config):
        fastq_screen_path = config["fastq_screen"]["path"]
        bowtie2_path = config["fastq_screen"]["path_to_bowtie2"]
        fqs_config_path = config["fastq_screen"]["path_to_config"]
    try:
        fastq_screen_cl = "{fastq_screen_path}
    except KeyError as e:
        ## FIXME check this e.args[0] thing I'm just faking it
        raise ValueError('Could not get required value "{}" from config file'.format(e.args[0]))
