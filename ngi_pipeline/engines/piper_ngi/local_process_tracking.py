import collections
import glob
import inspect
import os
import psutil
import re
import time

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.communication import mail_analysis
from ngi_pipeline.engines.piper_ngi.database import SampleAnalysis, get_db_session
from ngi_pipeline.engines.piper_ngi.utils import create_exit_code_file_path, \
                                                 create_project_obj_from_analysis_log, \
                                                 get_finished_seqruns_for_sample
from ngi_pipeline.engines.piper_ngi.results_parsers import parse_qualimap_coverage
from ngi_pipeline.utils.slurm import get_slurm_job_status, \
                                     kill_slurm_job_by_id
from ngi_pipeline.utils.parsers import STHLM_UUSNP_SEQRUN_RE, \
                                       STHLM_UUSNP_SAMPLE_RE
from sqlalchemy.exc import IntegrityError, OperationalError
from ngi_pipeline.utils.classes import with_ngi_config


LOG = minimal_logger(__name__)

@with_ngi_config
def update_charon_with_local_jobs_status(quiet=False, config=None, config_file_path=None):
    """Check the status of all locally-tracked jobs and update Charon accordingly.
    """
    if quiet and not config.get("quiet"):
        config['quiet'] = True
    LOG.info("Updating Charon with the status of all locally-tracked jobs...")
    with get_db_session() as session:
        charon_session = CharonSession()
        for sample_entry in session.query(SampleAnalysis).all():
            # Local names
            workflow = sample_entry.workflow
            project_name = sample_entry.project_name
            project_id = sample_entry.project_id
            project_base_path = sample_entry.project_base_path
            sample_id = sample_entry.sample_id
            engine = sample_entry.engine
            # Only one of these will have a value
            slurm_job_id = sample_entry.slurm_job_id
            process_id = sample_entry.process_id
            piper_exit_code = get_exit_code(workflow_name=workflow,
                                            project_base_path=project_base_path,
                                            project_name=project_name,
                                            project_id=project_id,
                                            sample_id=sample_id)
            label = "project/sample {}/{}".format(project_name, sample_id)

            if workflow not in ("merge_process_variantcall", "genotype_concordance",):
                LOG.error('Unknown workflow "{}" for {}; cannot update '
                          'Charon. Skipping sample.'.format(workflow, label))
                continue

            try:
                project_obj = create_project_obj_from_analysis_log(project_name,
                                                                   project_id,
                                                                   project_base_path,
                                                                   sample_id,
                                                                   workflow)
            except IOError as e: # analysis log file is missing!
                error_text = ('Could not find analysis log file! Cannot update '
                              'Charon for {} run {}/{}: {}'.format(workflow,
                                                                   project_id,
                                                                   sample_id,
                                                                   e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_name,
                                  sample_name=sample_id,
                                  engine_name=engine,
                                  level="ERROR",
                                  info_text=error_text,
                                  workflow=workflow)
                continue
            try:
                if piper_exit_code == 0:
                    # 0 -> Job finished successfully
                    set_status = "ANALYZED" # For the sample level
                    recurse_status = "DONE" # For the seqrun level
                    info_text = ('Workflow "{}" for {} finished succesfully. '
                                 'Recording status {} in Charon'.format(workflow,
                                                                        label,
                                                                        set_status))
                    LOG.info(info_text)
                    if not config.get('quiet'):
                        mail_analysis(project_name=project_name,
                                      sample_name=sample_id,
                                      engine_name=engine,
                                      level="INFO",
                                      info_text=info_text,
                                      workflow=workflow)
                    if workflow == "merge_process_variantcall":
                        status_field = "analysis_status"
                    elif workflow == "genotype_concordance":
                        status_field = "genotype_status"
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 **{status_field: set_status})
                    recurse_status_for_sample(project_obj,
                                              status_field=status_field,
                                              status_value=recurse_status,
                                              config=config)
                    # Job is only deleted if the Charon status update succeeds
                    # (if Charon is updated for this workflow)
                    session.delete(sample_entry)
                    if workflow == "merge_process_variantcall":
                        # Parse seqrun output results / update Charon
                        # This is a semi-optional step -- failure here will send an
                        # email but not more than once. The record is still removed
                        # from the local jobs database, so this will have to be done
                        # manually if you want it done at all.
                        piper_qc_dir = os.path.join(project_base_path, "ANALYSIS",
                                                    project_id, "piper_ngi",
                                                    "02_preliminary_alignment_qc")
                        update_coverage_for_sample_seqruns(project_id, sample_id,
                                                           piper_qc_dir)
                elif type(piper_exit_code) is int and piper_exit_code > 0:
                    # 1 -> Job failed
                    set_status = "FAILED"
                    error_text = ('Workflow "{}" for {} failed. Recording status '
                                  '{} in Charon.'.format(workflow, label, set_status))
                    LOG.error(error_text)
                    if not config.get('quiet'):
                        mail_analysis(project_name=project_name,
                                      sample_name=sample_id,
                                      engine_name=engine,
                                      level="ERROR",
                                      info_text=error_text,
                                      workflow=workflow)
                    if workflow == "merge_process_variantcall":
                        status_field = "analysis_status"
                    elif workflow == "genotype_concordance":
                        status_field = "genotype_status"
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 **{status_field: set_status})
                    recurse_status_for_sample(project_obj, status_field=status_field,
                                              status_value=set_status, config=config)
                    # Job is only deleted if the Charon update succeeds
                    # (if Charon is updated for this workflow)
                    session.delete(sample_entry)
                else:
                    # None -> Job still running OR exit code was never written (failure)
                    JOB_FAILED = None
                    if slurm_job_id:
                        try:
                            slurm_exit_code = get_slurm_job_status(slurm_job_id)
                        except ValueError as e:
                            slurm_exit_code = 1
                        if slurm_exit_code is not None: # "None" indicates job is still running
                            JOB_FAILED = True
                    else:
                        if not psutil.pid_exists(process_id):
                            # Job did not write an exit code and is also not running
                            JOB_FAILED = True
                    if JOB_FAILED:
                        set_status = "FAILED"
                        error_text = ('No exit code found but job not running '
                                      'for {} / {}: setting status to {} in '
                                      'Charon'.format(label, workflow, set_status))
                        if slurm_job_id:
                            exit_code_file_path = \
                                create_exit_code_file_path(workflow_subtask=workflow,
                                                           project_base_path=project_base_path,
                                                           project_name=project_name,
                                                           project_id=project_id,
                                                           sample_id=sample_id)
                            error_text += (' (slurm job id "{}", exit code file path '
                                           '"{}")'.format(slurm_job_id, exit_code_file_path))
                        LOG.error(error_text)
                        if not config.get('quiet'):
                            mail_analysis(project_name=project_name,
                                          sample_name=sample_id,
                                          engine_name=engine, level="ERROR",
                                          info_text=error_text,
                                          workflow=workflow)
                        if workflow == "merge_process_variantcall":
                            status_field = "analysis_status"
                        elif workflow == "genotype_concordance":
                            status_field = "genotype_status"
                        charon_session.sample_update(projectid=project_id,
                                                     sampleid=sample_id,
                                                     **{status_field: set_status})
                        recurse_status_for_sample(project_obj,
                                                  status_field=status_field,
                                                  status_value=set_status,
                                                  config=config)
                        # Job is only deleted if the Charon update succeeds
                        LOG.debug("Deleting local entry {}".format(sample_entry))
                        session.delete(sample_entry)
                    else: # Job still running
                        set_status = "UNDER_ANALYSIS"
                        if workflow == "merge_process_variantcall":
                            status_field = "alignment_status"
                        elif workflow == "genotype_concordance":
                            status_field = "genotype_status"
                        try:
                            charon_status = \
                                    charon_session.sample_get(projectid=project_id,
                                                              sampleid=sample_id).get(status_field)
                            if charon_status and not charon_status == set_status:
                                LOG.warn('Tracking inconsistency for {}: Charon status '
                                         'for field "{}" is "{}" but local process tracking '
                                         'database indicates it is running. Setting value '
                                         'in Charon to {}.'.format(label, status_field,
                                                                   charon_status, set_status))
                                charon_session.sample_update(projectid=project_id,
                                                             sampleid=sample_id,
                                                             **{status_field: set_status})
                                recurse_status_for_sample(project_obj,
                                                          status_field=status_field,
                                                          status_value="RUNNING",
                                                          config=config)
                        except CharonError as e:
                            error_text = ('Unable to update/verify Charon field '
                                          '"{}" for {} as "{}": {}'.format(status_field,
                                                                           label,
                                                                           set_status,
                                                                           e))
                            LOG.error(error_text)
                            if not config.get('quiet'):
                                mail_analysis(project_name=project_name, sample_name=sample_id,
                                              engine_name=engine, level="ERROR",
                                              workflow=workflow, info_text=error_text)
            except CharonError as e:
                error_text = ('Unable to update Charon field "{}" for {}: '
                              '{}'.format(status_field, label, e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_name, sample_name=sample_id,
                                  engine_name=engine, level="ERROR",
                                  workflow=workflow, info_text=error_text)
            except OSError as e:
                error_text = ('Permissions error when trying to update Charon '
                              '"{}" status for "{}": {}'.format(workflow, label, e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_name, sample_name=sample_id,
                                  engine_name=engine, level="ERROR",
                                  workflow=workflow, info_text=error_text)
        session.commit()



@with_ngi_config
def recurse_status_for_sample(project_obj, status_field, status_value, update_done=False,
                              extra_args=None, config=None, config_file_path=None):
    """Set seqruns under sample to have status for field <status_field> to <status_value>
    """

    if not extra_args:
        extra_args = {}
    extra_args.update({status_field: status_value})
    charon_session = CharonSession()
    project_id = project_obj.project_id
    for sample_obj in project_obj:
        # There's only one sample but this is an iterator so we pretend to loop
        sample_id = sample_obj.name
        for libprep_obj in sample_obj:
            libprep_id = libprep_obj.name
            for seqrun_obj in libprep_obj:
                seqrun_id = seqrun_obj.name
                label = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
                LOG.info('Updating status for field "{}" of project/sample/libprep/seqrun '
                         '"{}" to "{}" in Charon '.format(status_field, label, status_value))
                try:
                    charon_session.seqrun_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 libprepid=libprep_id,
                                                 seqrunid=seqrun_id,
                                                 **extra_args)
                except CharonError as e:
                    error_text = ('Could not update {} for project/sample/libprep/seqrun '
                                  '"{}" in Charon to "{}": {}'.format(status_field,
                                                                      label,
                                                                      status_value,
                                                                      e))
                    LOG.error(error_text)
                    if not config.get('quiet'):
                        mail_analysis(project_name=project_id, sample_name=sample_obj.name,
                                      level="ERROR", info_text=error_text, workflow=status_field)



@with_ngi_config
def update_coverage_for_sample_seqruns(project_id, sample_id, piper_qc_dir,
                                       config=None, config_file_path=None):
    """Find all the valid seqruns for a particular sample, parse their
    qualimap output files, and update Charon with the mean autosomal
    coverage for each.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises RuntimeError: If you specify both the seqrun_id and fcid and they don't match
    :raises ValueError: If arguments are incorrect
    """
    seqruns_by_libprep = get_finished_seqruns_for_sample(project_id, sample_id)

    charon_session = CharonSession()
    for libprep_id, seqruns in seqruns_by_libprep.iteritems():
        for seqrun_id in seqruns:
            label = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
            ma_coverage = _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id)
            LOG.info('Updating project/sample/libprep/seqrun "{}" in '
                     'Charon with mean autosomal coverage "{}"'.format(label, ma_coverage))
            try:
                charon_session.seqrun_update(projectid=project_id,
                                             sampleid=sample_id,
                                             libprepid=libprep_id,
                                             seqrunid=seqrun_id,
                                             mean_autosomal_coverage=ma_coverage)
            except CharonError as e:
                error_text = ('Could not update project/sample/libprep/seqrun "{}" '
                              'in Charon with mean autosomal coverage '
                              '"{}": {}'.format(label, ma_coverage, e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_id, sample_name=sample_id,
                                  engine_name="piper_ngi", level="ERROR", info_text=error_text)


def parse_mean_autosomal_coverage_for_sample(piper_qc_dir, sample_id):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample as gleaned from the qualimapReport.html present in
    piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :returns: The mean autosomal coverage
    :rtype: int
    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    return _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id)


def _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id=None, fcid=None):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample OR seqrun (if seqrun_id is passed) as gleaned from
    the qualimapReport.html present in piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)
    :param str seqrun_id: The run id (e.g. 140821_D00458_0029_AC45JGANXX) (optional) (specify either this or fcid)
    :param str fcid: The FCID (optional) (specify either this or seqrun_id)

    :returns: The mean autosomal coverage
    :rtype: int

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    try:
        if seqrun_id and fcid and (fcid != seqrun_id.split("_")[3]):
            raise ValueError(('seqrun_id and fcid both passed as arguments but do not '
                              'match (seqrun_id: "{}", fcid: "{}")'.format(seqrun_id, fcid)))
        if seqrun_id:
            piper_run_id = seqrun_id.split("_")[3]
        elif fcid:
            piper_run_id = fcid
        else:
            piper_run_id = None
    except IndexError:
        raise ValueError('Can\'t parse FCID from run id ("{}")'.format(seqrun_id))
    # Find all the appropriate files
    try:
        os.path.isdir(piper_qc_dir) and os.listdir(piper_qc_dir)
    except OSError as e:
        raise OSError('Piper result directory "{}" inaccessible when updating stats to Charon: {}.'.format(piper_qc_dir, e))
    piper_qc_dir_base = "{}.{}.{}".format(sample_id, (piper_run_id or "*"), sample_id)
    piper_qc_path = "{}*/".format(os.path.join(piper_qc_dir, piper_qc_dir_base))
    piper_qc_dirs = glob.glob(piper_qc_path)
    if not piper_qc_dirs: # Something went wrong, is the sample name with a hyphen or with an underscore ?
        piper_qc_dir_base = "{}.{}.{}".format(sample_id.replace('_', '-', 1), (piper_run_id or "*"), sample_id.replace('_', '-', 1))
        piper_qc_path = "{}*/".format(os.path.join(piper_qc_dir, piper_qc_dir_base))
        piper_qc_dirs = glob.glob(piper_qc_path)
        if not piper_qc_dirs: # Something went wrong in the alignment or we can't parse the file format
            raise OSError('Piper qc directories under "{}" are missing or in an unexpected format when updating stats to Charon.'.format(piper_qc_path))
    mean_autosomal_coverage = 0
    # Examine each lane and update the dict with its alignment metrics
    for qc_lane in piper_qc_dirs:
        genome_result = os.path.join(qc_lane, "genome_results.txt")
        # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
        if not os.path.isfile(genome_result):
            raise OSError('File "genome_results.txt" is missing from Piper result directory "{}"'.format(piper_qc_dir))
        # Get the alignment results for this lane
        mean_autosomal_coverage += parse_qualimap_coverage(genome_result)
    return mean_autosomal_coverage


@with_ngi_config
def record_process_sample(project, sample, workflow_subtask, analysis_module_name,
                          process_id=None, slurm_job_id=None, config=None, config_file_path=None):
    LOG.info('Recording slurm job id "{}" for project "{}", sample "{}", '
             'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
    with get_db_session() as session:
        sample_db_obj = SampleAnalysis(project_id=project.project_id,
                                       project_name=project.name,
                                       project_base_path=project.base_path,
                                       sample_id=sample.name,
                                       engine=analysis_module_name,
                                       workflow=workflow_subtask,
                                       process_id=process_id,
                                       slurm_job_id=slurm_job_id)
        try:
            session.add(sample_db_obj)
            for attempts in range(3):
                try:
                    session.commit()
                    LOG.info('Successfully recorded slurm job id "{}" for project "{}", sample "{}", '
                             'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
                    break
                except OperationalError as e:
                    LOG.warn('Database locked ("{}"). Waiting...'.format(e))
                    time.sleep(15)
            else:
                raise RuntimeError("Could not write to database after three attempts (locked?)")
        except (IntegrityError, RuntimeError) as e:
            raise RuntimeError('Could not record slurm job id "{}" for project "{}", '
                               'sample "{}", workflow "{}": {}'.format(slurm_job_id,
                                                                       project,
                                                                       sample,
                                                                       workflow_subtask,
                                                                       e.message))
        if workflow_subtask == "merge_process_variantcall":
            status_field = "analysis_status"
        elif workflow_subtask == "genotype_concordance":
            status_field = "genotype_status"
        else:
            raise ValueError('Charon field for workflow "{}" unknown; '
                             'cannot update Charon.'.format(workflow_subtask))
        try:
            set_status = "UNDER_ANALYSIS"
            LOG.info('Updating Charon status for project/sample '
                     '{}/{} to {}'.format(project, sample, set_status))
            CharonSession().sample_update(projectid=project.project_id,
                                          sampleid=sample.name,
                                          **{status_field: set_status})
            project_obj = create_project_obj_from_analysis_log(project.name,
                                                               project.project_id,
                                                               project.base_path,
                                                               sample.name,
                                                               workflow_subtask)
            recurse_status_for_sample(project_obj,
                                      status_field=status_field,
                                      status_value="RUNNING",
                                      extra_args={'mean_autosomal_coverage': 0},
                                      config=config)
        except CharonError as e:
            error_text = ('Could not update Charon status for {} for project/sample '
                          '{}/{} due to error: {}'.format(status_field, project, sample, e))

            LOG.error(error_text)
            if not config.get('quiet'):
                mail_analysis(project_name=project.project_id,
                              sample_name=sample.name,
                              engine_name='piper_ngi',
                              level="ERROR",
                              info_text=error_text,
                              workflow=workflow_subtask)


def is_sample_analysis_running_local(workflow_subtask, project_id, sample_id):
    """Determine if a sample is currently being analyzed by accessing the local
    process tracking database."""
    sample_run_name = "{}/{}".format(project_id, sample_id)
    LOG.info('Checking if sample run "{}" is currently being analyzed '
             '(workflow "{}")...'.format(sample_run_name, workflow_subtask))
    with get_db_session() as session:
        db_q = session.query(SampleAnalysis).filter_by(workflow=workflow_subtask,
                                                       project_id=project_id,
                                                       sample_id=sample_id)
        if session.query(db_q.exists()).scalar():
            LOG.info('..."{}" for sample "{}" is currently being '
                     'analyzed.'.format(workflow_subtask, sample_run_name))
            return True
        else:
            LOG.info('..."{}" for sample "{}" is not currently under '
                     'analysis.'.format(workflow_subtask, sample_run_name))
            return False


def kill_running_sample_analysis(workflow_subtask, project_id, sample_id):
    """Determine if a sample is currently being analyzed by accessing the local
    process tracking database."""
    sample_run_name = "{}/{}".format(project_id, sample_id)
    LOG.info('Attempting to kill sample analysis run "{}"'.format(sample_run_name))
    LOG.info('Checking if sample run "{}" is currently being analyzed '
             '(workflow "{}")...'.format(sample_run_name, workflow_subtask))
    with get_db_session() as session:
        db_q = session.query(SampleAnalysis).filter_by(workflow=workflow_subtask,
                                                       project_id=project_id,
                                                       sample_id=sample_id)
        sample_run = db_q.first()
        if sample_run:
            try:
                slurm_job_id = sample_run.slurm_job_id
                LOG.info('...sample run "{}" is currently being analyzed '
                         '(workflow subtask "{}") and has slurm job id "{}"; '
                         'trying to kill it...'.format(sample_run_name,
                                                       workflow_subtask,
                                                       slurm_job_id))
                kill_slurm_job_by_id(slurm_job_id)
            except Exception as e:
                LOG.error('Could not kill sample run "{}": {}'.format(sample_run_name, e))
                return False
            try:
                project_obj = create_project_obj_from_analysis_log(sample_run.project_name,
                                                                   sample_run.project_id,
                                                                   sample_run.project_base_path,
                                                                   sample_run.sample_id,
                                                                   sample_run.workflow)
            except IOError as e: # analysis log file is missing!
                error_text = ('Could not find analysis log file! Cannot update '
                              'Charon for {} run {}/{}: {}'.format(sample_run.workflow,
                                                                   sample_run.project_id,
                                                                   sample_run.sample_id,
                                                                   e))
                LOG.error(error_text)
            else:
                try:
                    charon_session = CharonSession()
                    set_status = "FAILED"
                    if workflow_subtask == "genotype_concordance":
                        status_field = "genotype_status"
                    elif workflow_subtask == "merge_process_variantcall":
                        status_field = "analysis_status"
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 **{status_field: set_status})
                    recurse_status_for_sample(project_obj,
                                              status_field=status_field,
                                              status_value=set_status)
                except CharonError as e:
                    LOG.error('Couldn\'t update Charon field "{}" to "{} for '
                              'project/sample "{}/{}"'.format(status_field, set_status,
                                                              project_id, sample_id))
            try:
                LOG.info('Removing sample run "{}" from local jobs database...'.format(sample_run_name))
                # Remove from local jobs database
                session.delete(sample_run)
                session.commit()
                LOG.info("Deleted.")
            except Exception as e:
                LOG.error('Failed to remove entry for sample run "{}" from '
                          'local jobs database: {}'.format(sample_run_name, e))
        else:
            LOG.info('...sample run "{}" is not currently under analysis.'.format(sample_run_name))
    return True


def get_exit_code(workflow_name, project_base_path, project_name, project_id,
                  sample_id, libprep_id=None, seqrun_id=None):
    exit_code_file_path = create_exit_code_file_path(workflow_name,
                                                     project_base_path,
                                                     project_name,
                                                     project_id,
                                                     sample_id,
                                                     libprep_id,
                                                     seqrun_id)

    try:
        with open(exit_code_file_path, 'r') as f:
            exit_code = f.read().strip()
            if exit_code:
                exit_code = int(exit_code)
            return exit_code
    except IOError as e:
        return None     # Process is not yet complete
    except ValueError as e:
        raise ValueError('Could not determine job exit status: not an integer ("{}")'.format(e))
    else:
        return None
