from ngi_pipeline.piper_ngi.database import get_db_session, SeqrunAnalysis, SampleAnalysis
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.parsers import STHLM_UUSNP_SEQRUN_RE, STHLM_UUSNP_SAMPLE_RE


LOG = minimal_logger(__name__)


def record_process_seqrun(project, sample, libprep, seqrun, workflow_name,
                          analysis_module_name, analysis_dir, pid):
    LOG.info('Recording process id "{}" for project "{}", sample "{}", libprep "{}", '
             'seqrun "{}", workflow "{}"'.format(pid, project, sample, libprep,
                                                 seqrun, workflow_name))
    session = get_db_session()
    seqrun_db_obj = SeqrunAnalysis(project_id=project.project_id,
                                   project_name=project.name,
                                   project_base_path=project.base_path,
                                   sample_id=sample.name,
                                   libprep_id=libprep.name,
                                   seqrun_id=seqrun.name,
                                   engine=analysis_module_name,
                                   workflow=workflow_name,
                                   analysis_dir=analysis_dir,
                                   process_id=pid)
    session.add(seqrun_db_obj)
    session.commit()
    LOG.info('Successfully recorded process id "{}" for project "{}", sample "{}", '
             'libprep "{}", seqrun "{}", workflow "{}"'.format(pid,
                                                               project,
                                                               sample,
                                                               libprep,
                                                               seqrun,
                                                               workflow_name))


def update_local_jobs_status():
    session = get_db_session()
    for seqrun_entry in session.query(SeqrunAnalysis).all():
        import ipdb; ipdb.set_trace()
        exit_code = get_exit_code(workflow_name=seqrun_entry.workflow)
    for sample_entry in session.query(SampleAnalysis).all():
        import ipdb; ipdb.set_trace()


def record_process_sample(project, sample, workflow_name, analysis_module_name,
                          analysis_dir, pid, config=None):
    LOG.info('Recording process id "{}" for project "{}", sample "{}", '
             'workflow "{}"'.format(pid, project, sample, workflow_name))
    session = get_db_session()
    seqrun_db_obj = SampleAnalysis(project_id=project.project_id,
                                   project_name=project.name,
                                   project_base_path=project.base_path,
                                   sample_id=sample.name,
                                   engine=analysis_module_name,
                                   workflow=workflow_name,
                                   analysis_dir=analysis_dir,
                                   process_id=pid)
    session.add(seqrun_db_obj)
    session.commit()
    LOG.info('Successfully recorded process id "{}" for project "{}", sample "{}", '
             'workflow "{}"'.format(pid, project, sample, libprep, seqrun, workflow_name))


## Is it worth checking the database even? Maybe the better option is to check
## for the file exit code and then update the local db if needed
def is_seqrun_analysis_running_local(workflow, project, sample, libprep, seqrun):
    """Determine if a flowcell is currently being analyzed by accessing the local
    process tracking database.

    :param str workflow: The workflow name
    :param NGIProject project: The NGIProject object
    :param NGISample sample: The NGISample object
    :param NGILibraryPrep libprep: The NGILibraryPrep object
    :param NGISeqRun seqrun: The NGISeqRun object

    :returns: True if under analysis, False otherwise
    """
    sequencing_run = "{}/{}/{}/{}".format(project.project_id, sample, libprep, seqrun)
    LOG.info('Checking if sequencing run "{}" is currently '
             'being analyzed...'.format(sequencing_run))
    session = get_db_session()
    db_q = session.query(SeqrunAnalysis).filter_by(workflow=workflow,
                                                   project_id=project.project_id,
                                                   sample_id=sample.name,
                                                   libprep_id=libprep.name,
                                                   seqrun_id=seqrun.name)
    if session.query(db_q.exists()).scalar():
        LOG.info('...sequencing run "{}" is currently being analyzed.'.format(sequencing_run))
        return True
    else:
        LOG.info('...sequencing run "{}" is not currently under analysis.'.format(sequencing_run))
        return False

def is_sample_analysis_running_local(workflow, project, sample):
    """Determine if a sample is currently being analyzed by accessing the local
    process tracking database."""



def get_exit_code(workflow_name, project, sample, libprep=None, seqrun=None):
    exit_code_file_path = create_exit_code_file_path(workflow_name, project, sample, libprep, seqrun)
    try:
        with open(exit_code_file_path, 'r') as f:
            exit_code = int(f.read().strip())
            return exit_code
    except OSError as e:
        if e.errno == 2:    # No such file or directory
            return None     # Process is not yet complete
    except ValueError as e:
        raise ValueError('Could not determine job exit status: not an integer ("{}")'.format(e))
