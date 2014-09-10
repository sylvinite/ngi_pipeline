from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.piper_ngi.database import get_db_session, SeqrunAnalysis, SampleAnalysis
from ngi_pipeline.piper_ngi.utils import create_exit_code_file_path
from ngi_pipeline.utils.parsers import STHLM_UUSNP_SEQRUN_RE, STHLM_UUSNP_SAMPLE_RE


LOG = minimal_logger(__name__)


def update_charon_with_local_jobs_status():
    session = get_db_session()
    charon_session = CharonSession()

    # Sequencing Run Analyses
    for seqrun_entry in session.query(SeqrunAnalysis).all():

        # Local names
        workflow = seqrun_entry.workflow
        project_name = seqrun_entry.project_name
        project_id = seqrun_entry.project_id
        project_base_path = seqrun_entry.project_base_path
        sample_id = seqrun_entry.sample_id
        libprep_id = seqrun_entry.libprep_id
        seqrun_id = seqrun_entry.seqrun_id

        exit_code = get_exit_code(workflow_name=workflow,
                                  project_base_path=project_base_path,
                                  project_name=project_name,
                                  sample_id=sample_id,
                                  libprep_id=libprep_id,
                                  seqrun_id=seqrun_id)
        if exit_code == 0:
            # 0 -> Job finished successfully
            ## Need to somehow casecade status levels down from seqrun->libprep->sample->project
            charon_session.seqrun_update(projectid=project_id,
                                         sampleid=sample_id,
                                         libprepid=libprep_id,
                                         seqrunid=seqrun_id,
                                         alignment_status="DONE")
            ## TODO To be implemented
            #write_workflow_results_to_charon(workflow=workflow,
            #                                 base_path=project_base_path,
            #                                 project_name=project_name,
            #                                 sample_name=sample_name,
            #                                 libprep_id=libprep_id,
            #                                 seqrun_id=seqrun_id)
        elif exit_code == 1:
            # 1 -> Job failed (DATA_FAILURE / COMPUTATION_FAILURE ?)
            charon_session.seqrun_update(projectid=project_id,
                                         sampleid=sample_id,
                                         libprepid=libprep_id,
                                         seqrunid=seqrun_id,
                                         alignment_status="FAILED")
            session.delete(seqrun_entry)
        else:
            # None -> Job still running
            charon_status = charon_session.seqrun_get(projectid=project_id,
                                                      sampleid=sample_id,
                                                      libprepid=libprep_id,
                                                      seqrunid=seqrun_id)['alignment_status']
            if not charon_status == "RUNNING":
                LOG.warn('Tracking inconsistency for project "{}" / sample "{}" '
                         'libprep "{}" / seqrun "{}": Charon status is "{}" but '
                         'local process tracking database indicates it is running. '
                         'Setting value in Charon to RUNNING.'.format(project_name,
                                                                      sample_id,
                                                                      libprep_id,
                                                                      seqrun_id,
                                                                      charon_status))
                charon_session.seqrun_update(project_id=project_id,
                                             sample_id=sample_id,
                                             libprep_id=libprep_id,
                                             seqrun_id=seqrun_id,
                                             alignment_status="RUNNING")
    for sample_entry in session.query(SampleAnalysis).all():

        # Local names
        workflow = sample_entry.workflow
        project_name = sample_entry.project_name
        project_id = sample_entry.project_id
        project_base_path = sample_entry.project_base_path
        sample_id = sample_entry.sample_id

        exit_code = get_exit_code(workflow_name=workflow,
                                  project_base_path=project_base_path,
                                  project_name=project_name,
                                  sample_id=sample_id)
        if exit_code == 0:
            # 0 -> Job finished successfully
            ## Need to somehow casecade status levels down from seqrun->libprep->sample->project
            charon_session.sample_update(projectid=project_id,
                                         sampleid=sample_id,
                                         status="DONE")
            ## TODO To be implemented
            write_workflow_results_to_charon(workflow=workflow,
                                             base_path=project_base_path,
                                             project_name=project_name,
                                             sample_name=sample_name)
        elif exit_code == 1:
            # 1 -> Job failed (DATA_FAILURE / COMPUTATION_FAILURE ?)
            charon_session.sample_update(projectid=project_id,
                                         sampleid=sample_id,
                                         status="FAILED")
            session.delete(sample_entry)
        else:
            # None -> Job still running
            charon_status = charon_session.sample_get(projectid=project_id,
                                                      sampleid=sample_id)['status']
            if not charon_status == "RUNNING":
                LOG.warn('Tracking inconsistency for project "{}" / sample "{}": '
                         'Charon status is "{}" but local process tracking '
                         'database indicates it is running. '
                         'Setting value in Charon to RUNNING.'.format(project_name,
                                                                      sample_id,
                                                                      charon_status))
                charon_session.seqrun_update(projectid=project_id,
                                             sampleid=sample_id,
                                             status="RUNNING")


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


# Do we need this function?
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

# Do we need this function?
def is_sample_analysis_running_local(workflow, project, sample):
    """Determine if a sample is currently being analyzed by accessing the local
    process tracking database."""
    sample_run_name = "{}/{}".format(project.project_id, sample)
    LOG.info('Checking if sample run "{}" is currently '
             'being analyzed...'.format(sample_run_name))
    session = get_db_session()
    db_q = session.query(SampleAnalysis).filter_by(workflow=workflow,
                                                   project_id=project.project_id,
                                                   sample_id=sample.name)
    if session.query(db_q.exists()).scalar():
        LOG.info('...sample run "{}" is currently being analyzed.'.format(sample_run_name))
        return True
    else:
        LOG.info('...sample run "{}" is not currently under analysis.'.format(sample_run_name))
        return False



def get_exit_code(workflow_name, project_base_path, project_name,
                  sample_id, libprep_id=None, seqrun_id=None):
    exit_code_file_path = create_exit_code_file_path(workflow_name,
                                                     project_base_path,
                                                     project_name,
                                                     sample_id,
                                                     libprep_id,
                                                     seqrun_id)
    try:
        with open(exit_code_file_path, 'r') as f:
            exit_code = int(f.read().strip())
            return exit_code
    except IOError as e:
        if e.errno == 2:    # No such file or directory
            return None     # Process is not yet complete
    except ValueError as e:
        raise ValueError('Could not determine job exit status: not an integer ("{}")'.format(e))
