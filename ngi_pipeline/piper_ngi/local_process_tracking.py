from ngi_pipeline.piper_ngi.database import get_db_session, SeqrunAnalysis, SampleAnalysis
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.parsers import STHLM_UUSNP_SEQRUN_RE, STHLM_UUSNP_SAMPLE_RE


LOG = minimal_logger(__name__)


def record_process_seqrun(project, sample, libprep, seqrun, workflow_name,
                          analysis_module_name, analysis_dir, pid, config=None):
    LOG.info('Recording process id "{}" for project "{}", sample "{}", libprep "{}", '
             'seqrun "{}", workflow "{}"'.format(pid, project, sample, libprep,
                                                 seqrun, workflow_name))
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
    LOG.info('Successfully recorded process id "{}" for project "{}", sample "{}", '
             'libprep "{}", seqrun "{}", workflow "{}"'.format(pid,
                                                               project,
                                                               sample,
                                                               libprep,
                                                               seqrun,
                                                               workflow_name))


def record_process_sample(project, sample, workflow_name, analysis_module_name,
                          analysis_dir, pid, config=None):
    LOG.info('Recording process id "{}" for project "{}", sample "{}", '
             'workflow "{}"'.format(pid, project, sample, workflow_name))
    session = get_db_session()
    seqrun_db_obj = SampleAnalysis(project_id=project.project_id,
                                   project_name=project.name,
                                   sample_id=sample.name,
                                   engine=analysis_module_name,
                                   workflow=workflow_name,
                                   analysis_dir=analysis_dir,
                                   process_id=pid)
    session.add(seqrun_db_obj)
    session.commit()
    LOG.info('Successfully recorded process id "{}" for project "{}", sample "{}", '
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
