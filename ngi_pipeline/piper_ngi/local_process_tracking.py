from ngi_pipeline.piper_ngi.database import get_db_session, SeqrunAnalysis
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.parsers import STHLM_UUSNP_SEQRUN_RE, STHLM_UUSNP_SAMPLE_RE


LOG = minimal_logger(__name__)


def record_process_seqrun(project, sample, libprep, seqrun, workflow_name,
                          analysis_module_name, analysis_dir, pid, config=None):
    LOG.info('Recording process id "{}" for project "{}", sample "{}", seqrun "{}" '
             'workflow "{}"'.format(pid, project, sample, seqrun, workflow_name))
    session = get_db_session()
    seqrun_db_obj = SeqrunAnalysis(project_id=project.project_id,
                                   project_name=project.name,
                                   sample_id=sample.name,
                                   libprep_id=libprep.name,
                                   seqrun_id=seqrun.name,
                                   engine=analysis_module_name,
                                   workflow=workflow_name,
                                   analysis_dir=analysis_dir,
                                   process_id=pid)
    session.add(seqrun_db_obj)
    session.commit()
    LOG.info('Successfully recorded process id "{}" for project "{}", sample "{}", libprep "{}", seqrun "{}",'
             'workflow "{}"'.format(pid, project, sample, libprep, seqrun, workflow_name))


## TODO Should we add "workflow" as a key?
def is_seqrun_analysis_running(project, sample, libprep, seqrun, config=None):
    """Determine if a flowcell is currently being analyzed."""
    sequencing_run = "{}/{}/{}/{}".format(project.project_id, sample, libprep, seqrun)
    LOG.info('Checking if sequencing run "{}" is currently '
             'being analyzed...'.format(sequencing_run))
    session = get_db_session()
    db_q = session.query(SeqrunAnalysis).filter_by(project_id=project.project_id,
                                                sample_id=sample.name,
                                                libprep_id=libprep.name,
                                                seqrun_id=seqrun.name)
    if session.query(db_q.exists()).scalar():
        LOG.info('...sequencing run "{}" is currently being analyzed.'.format(sequencing_run))
        return True
    else:
        LOG.info('...sequencing run "{}" is not currently under analysis.'.format(sequencing_run))
        return False


## MARIO FIXME
# This function should be used along with a Charon database check for sample status
# to ensure that there aren't flowcell analyses running
def is_sample_analysis_running(project, sample, config=None):
    """Determine if a sample is currently being analyzed."""
    return check_if_sample_analysis_is_running(project, sample, config)
    # Change to this once it's implemented
    #LOG.info("Checking if sample {}/{} is currently "
    #         "being analyzed...".format(project.project_id, sample))
    #return is_analysis_running(project, sample, level="sample")


## MARIO FIXME this doesn't check if there are flowcell analyses running
##             this is all horrible but will change when we move to SQL
##             and build a proper monitoring/syncing submodule
def check_if_sample_analysis_is_running(project, sample, config=None):
    """checks if a given project/sample is currently analysed. 
    Determines if a given sample is currently being analyzed using the local job tracking
    database as its source of information.

    :param NGIProject project: The Project object
    :param NGISample sample: The Sample object
    :param dict config: The parsed configuration file (optional)

    :raises RuntimeError: If the configuration file cannot be found

    :returns: True or False
    :rtype: bool
    """
    ### FIXME this works for now but obviously the lookups will be much easier when we move to the SQL database
    ### PRIORITY 2
    LOG.info("Checking if sample {}/{} is currently "
             "being analyzed...".format(project.project_id, sample))
    with get_shelve_database(config) as db:
        #get all keys, i.e., get all running projects
        running_processes = db.keys()
        #check that project_sample are not involved in any running process
        process_to_be_searched = "{}_{}".format(project, sample)
        for running_process in running_processes:
            try:
                m_dict = STHLM_UUSNP_SAMPLE_RE.match(running_process).groupdict()
            except AttributeError:
                raise ValueError("Could not extract information from jobid string \"{}\" and cannot continue.".format(running_process))
            project_name = m_dict['project_name']
            #project_id = get_project_id_from_name(project_name)
            sample_id  = m_dict['sample_id']
            #libprep_id = m_dict['libprep_id']
            #seqrun_id = m_dict['seqrun_id']
            #libprep_seqrun_id = "{}_{}".format(libprep_id, seqrun_id)

            project_sample = "{}_{}".format(project_name, sample_id)
            if project_sample == process_to_be_searched:
                LOG.info('...sample run "{}" is currently being analyzed.'.format(process_to_be_searched))
                return True
        #if I do not find hits I return False
        LOG.info('...sample run "{}" is not currently being analyzed.'.format(process_to_be_searched))
        return False

