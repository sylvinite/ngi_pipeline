import glob
import inspect
import os
import psutil
import re
import time

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.engines.piper_ngi.database import SeqrunAnalysis, SampleAnalysis, get_db_session
from ngi_pipeline.engines.piper_ngi.utils import create_exit_code_file_path
from ngi_pipeline.utils.parsers import get_slurm_job_status, \
                                       parse_qualimap_results, \
                                       STHLM_UUSNP_SEQRUN_RE, \
                                       STHLM_UUSNP_SAMPLE_RE


LOG = minimal_logger(__name__)


def update_charon_with_local_jobs_status():
    """Check the status of all locally-tracked jobs and update Charon accordingly.
    """
    LOG.info("Updating Charon with the status of all locally-tracked jobs...")
    with get_db_session() as session:
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
            slurm_job_id = seqrun_entry.slurm_job_id

            piper_exit_code = get_exit_code(workflow_name=workflow,
                                            project_base_path=project_base_path,
                                            project_name=project_name,
                                            sample_id=sample_id,
                                            libprep_id=libprep_id,
                                            seqrun_id=seqrun_id)
            label = "project/sample/libprep/seqrun {}/{}/{}/{}".format(project_name,
                                                                       sample_id,
                                                                       libprep_id,
                                                                       seqrun_id)
            try:
                if piper_exit_code == 0:
                    # 0 -> Job finished successfully
                    LOG.info('Workflow "{}" for {} finished succesfully. '
                             'Recording status "DONE" in Charon'.format(workflow, label))
                    set_alignment_status = "DONE"
                    try:
                        write_to_charon_alignment_results(base_path=project_base_path,
                                                          project_name=project_name,
                                                          project_id=project_id,
                                                          sample_id=sample_id,
                                                          libprep_id=libprep_id,
                                                          seqrun_id=seqrun_id)
                    except (RuntimeError, ValueError) as e:
                        LOG.error(e)
                        set_alignment_status = "FAILED"
                    charon_session.seqrun_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 libprepid=libprep_id,
                                                 seqrunid=seqrun_id,
                                                 alignment_status=set_alignment_status)
                    # Job is only deleted if the Charon update succeeds
                    session.delete(seqrun_entry)
                elif piper_exit_code == 1:
                    # 1 -> Job failed (DATA_FAILURE / COMPUTATION_FAILURE ?)
                    LOG.info('Workflow "{}" for {} failed. Recording status '
                             '"FAILED" in Charon.'.format(workflow, label))
                    charon_session.seqrun_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 libprepid=libprep_id,
                                                 seqrunid=seqrun_id,
                                                 alignment_status="FAILED")
                    # Job is only deleted if the Charon update succeeds
                    LOG.debug("Deleting local entry {}".format(seqrun_entry))
                    session.delete(seqrun_entry)
                else:
                    # None -> Job still running OR exit code was never written (failure)
                    try:
                        slurm_exit_code = get_slurm_job_status(slurm_job_id)
                    except ValueError as e:
                        slurm_exit_code = 1
                    if slurm_exit_code is not None: # None indicates job is still running
                        LOG.warn('No exit code found but job not running for '
                                '{}: setting status to "FAILED" in Charon'.format(label))
                        charon_session.seqrun_update(projectid=project_id,
                                                     sampleid=sample_id,
                                                     libprepid=libprep_id,
                                                     seqrunid=seqrun_id,
                                                     alignment_status="FAILED")
                        # Job is only deleted if the Charon update succeeds
                        LOG.debug("Deleting local entry {}".format(seqrun_entry))
                        session.delete(seqrun_entry)
                    else:
                        charon_status = charon_session.seqrun_get(projectid=project_id,
                                                                  sampleid=sample_id,
                                                                  libprepid=libprep_id,
                                                                  seqrunid=seqrun_id)['alignment_status']
                        if not charon_status == "RUNNING":
                            LOG.warn('Tracking inconsistency for {}: Charon status is "{}" but '
                                     'local process tracking database indicates it is running. '
                                     'Setting value in Charon to RUNNING.'.format(label, charon_status))
                            charon_session.seqrun_update(projectid=project_id,
                                                         sampleid=sample_id,
                                                         libprepid=libprep_id,
                                                         seqrunid=seqrun_id,
                                                         alignment_status="RUNNING")
            except CharonError as e:
                LOG.error('Unable to update Charon status for "{}": {}'.format(label, e))


        for sample_entry in session.query(SampleAnalysis).all():

            # Local names
            workflow = sample_entry.workflow
            project_name = sample_entry.project_name
            project_id = sample_entry.project_id
            project_base_path = sample_entry.project_base_path
            sample_id = sample_entry.sample_id
            slurm_job_id = sample_entry.slurm_job_id

            piper_exit_code = get_exit_code(workflow_name=workflow,
                                            project_base_path=project_base_path,
                                            project_name=project_name,
                                            sample_id=sample_id)

            label = "project/sample/libprep/seqrun {}/{}".format(project_name,
                                                                 sample_id)
            try:
                if piper_exit_code == 0:
                    # 0 -> Job finished successfully
                    LOG.info('Workflow "{}" for {} finished succesfully. '
                             'Recording status "DONE" in Charon'.format(workflow, label))
                    set_status = "DONE"
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 status=set_status)
                    # Job is only deleted if the Charon update succeeds
                    session.delete(sample_entry)
                elif piper_exit_code == 1:
                    # 1 -> Job failed (DATA_FAILURE / COMPUTATION_FAILURE ?)
                    LOG.info('Workflow "{}" for {} failed. Recording status '
                             '"COMPUTATION_FAILED" in Charon.'.format(workflow, label))
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 status="COMPUTATION_FAILED")
                    # Job is only deleted if the Charon update succeeds
                    session.delete(sample_entry)
                else:
                    # None -> Job still running OR exit code was never written (failure)
                    try:
                        slurm_exit_code = get_slurm_job_status(slurm_job_id)
                    except ValueError as e:
                        slurm_exit_code = 1
                    if slurm_exit_code is not None: # None indicates job is still running
                        LOG.warn('No exit code found but job not running for '
                                 '{}: setting status to "FAILED" in Charon'.format(label))
                        charon_session.sample_update(projectid=project_id,
                                                     sampleid=sample_id,
                                                     alignment_status="COMPUTATION_FAILED")
                        # Job is only deleted if the Charon update succeeds
                        LOG.debug("Deleting local entry {}".format(sample_entry))
                        session.delete(sample_entry)
                    else:
                        charon_status = charon_session.sample_get(projectid=project_id,
                                                              sampleid=sample_id)['status']
                        if not charon_status == "RUNNING":
                            LOG.warn('Tracking inconsistency for {}: Charon status is "{}" but '
                                     'local process tracking database indicates it is running. '
                                     'Setting value in Charon to RUNNING.'.format(label, charon_status))
                            charon_session.sample_update(projectid=project_id,
                                                         sampleid=sample_id,
                                                         status="RUNNING")
            except CharonError as e:
                LOG.error('Unable to update Charon status for "{}": {}'.format(label, e))
        session.commit()


def parse_mean_autosomal_coverage_for_seqrun(piper_qc_dir, sample_id, seqrun_id=None, fcid=None):
    """This will return an integer value representing the total autosomal coverage
    for a particular seqrun as gleaned from the qualimapReport.html present in
    piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)
    :param str seqrun_id: The run id (e.g. 140821_D00458_0029_AC45JGANXX) (optional) (specify either this or fcid)
    :param str fcid: The FCID (optional) (specify either this or seqrun_id)

    :returns: The mean autosomal coverage
    :rtype: int

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises RuntimeError: If you specify both the seqrun_id and fcid and they don't match
    :raises ValueError: If arguments are incorrect
    :raises TypeError: If you don't pass one of seqrun_id or fcid
    """
    if not (seqrun_id or fcid):
        raise TypeError('{}() requires a value for either "seqrun_id" or "fcid"'.format(inspect.stack()[0][3]))
    return _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id, fcid)


def parse_mean_autosomal_coverage_for_sample(piper_qc_dir, sample_id):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample as gleaned from the qualimapReport.html present in
    piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :returns: The mean autosomal coverage
    :rtype: int
    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises RuntimeError: If you specify both the seqrun_id and fcid and they don't match
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
    :raises RuntimeError: If you specify both the seqrun_id and fcid and they don't match
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


    seqrun_dict = {}
    seqrun_dict["lanes"] = 0
    # Find all the appropriate files
    try:
        os.path.isdir(piper_qc_dir) and os.listdir(piper_qc_dir)
    except OSError as e:
        raise OSError('Piper result directory "{}" inaccessible when updating stats to Charon: {}.'.format(piper_qc_dir, e))
    piper_qc_dir_base = "{}.{}.{}".format(sample_id, (piper_run_id or "*"), sample_id)
    piper_qc_path = "{}*/".format(os.path.join(piper_qc_dir, piper_qc_dir_base))
    piper_qc_dirs = glob.glob(piper_qc_path)
    if not piper_qc_dirs: # Something went wrong in the alignment or we can't parse the file format
        raise OSError('Piper qc directories under "{}" are missing or in an unexpected format when updating stats to Charon.'.format(piper_qc_path))
    # Examine each lane and update the dict with its alignment metrics
    for qc_lane in piper_qc_dirs:
        genome_result = os.path.join(qc_lane, "genome_results.txt")
        # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
        # We should flag this somehow and send an email at some point.
        if not os.path.isfile(genome_result):
            raise OSError('File "genome_results.txt" is missing from Piper result directory "{}"'.format(piper_qc_dir))
        # Get the alignment results for this lane
        lane_alignment_metrics = parse_qualimap_results(genome_result)
        # Update the dict for this lane
        update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics)
    return seqrun_dict["mean_autosomal_coverage"]


## TODO update this to use the function above
def write_to_charon_alignment_results(base_path, project_name, project_id, sample_id, libprep_id, seqrun_id):
    """Update the status of a sequencing run after alignment.

    :param str project_name: The name of the project (e.g. T.Durden_14_01)
    :param str project_id: The id of the project (e.g. P1171)
    :param str sample_id: ...
    :param str libprep_id: ...
    :param str seqrun_id: ...

    :raises RuntimeError: If the Charon database could not be updated
    :raises ValueError: If the output data could not be parsed.
    """
    charon_session = CharonSession()
    try:
        seqrun_dict = charon_session.seqrun_get(project_id, sample_id, libprep_id, seqrun_id)
    except CharonError as e:
        raise CharonError('Error accessing database for project "{}", sample {}; '
                           'could not update Charon while performing best practice: '
                           '{}'.format(project_name, sample_id,  e))
    piper_run_id = seqrun_id.split("_")[3]
    seqrun_dict["lanes"] = 0
    if seqrun_dict.get("alignment_status") == "DONE":
        LOG.warn("Sequencing run \"{}\" marked as DONE but writing new alignment results; "
                 "this will overwrite the previous results.".format(seqrun_id))
    # Find all the appropriate files
    piper_result_dir = os.path.join(base_path, "ANALYSIS", project_name, "02_preliminary_alignment_qc")
    try:
        os.path.isdir(piper_result_dir) and os.listdir(piper_result_dir)
    except OSError as e:
        raise ValueError("Piper result directory \"{}\" inaccessible when updating stats to Charon: {}.".format(piper_result_dir, e))
    piper_qc_dir_base = "{}.{}.{}".format(sample_id, piper_run_id, sample_id)
    piper_qc_path = "{}*/".format(os.path.join(piper_result_dir, piper_qc_dir_base))
    piper_qc_dirs = glob.glob(piper_qc_path)
    if not piper_qc_dirs: # Something went wrong in the alignment or we can't parse the file format
        raise ValueError("Piper qc directories under \"{}\" are missing or in an unexpected format when updating stats to Charon.".format(piper_qc_path))

    # Examine each lane and update the dict with its alignment metrics
    for qc_lane in piper_qc_dirs:
        genome_result = os.path.join(qc_lane, "genome_results.txt")
        # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
        # We should flag this somehow and send an email at some point.
        if not os.path.isfile(genome_result):
            raise ValueError("File \"genome_results.txt\" is missing from Piper result directory \"{}\"".format(piper_result_dir))
        # Get the alignment results for this lane
        lane_alignment_metrics = parse_qualimap_results(genome_result)
        # Update the dict for this lane
        update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics)
    try:
        # Update the seqrun in the Charon database
        charon_session.seqrun_update(**seqrun_dict)
    except CharonError as e:
        error_msg = ('Failed to update run alignment status for run "{}" in project {} '
                     'sample {}, library prep {} to  Charon database: {}'.format(seqrun_id,
                      project_name, sample_id, libprep_id, e))
        raise CharonError(error_msg)


# This works for one run
def update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics):
    num_lanes = seqrun_dict.get("lanes")    # This gives 0 the first time
    seqrun_dict["lanes"] = seqrun_dict["lanes"] + 1   # Increment
    current_lane = re.match(".+\.(\d)\.bam", lane_alignment_metrics["bam_file"]).group(1)

    fields_to_update = ('mean_coverage',
                        'std_coverage',
                        'aligned_bases',
                        'mapped_bases',
                        'mapped_reads',
                        'reads_per_lane',
                        'sequenced_bases',
                        'bam_file',
                        'output_file',
                        'GC_percentage',
                        'mean_mapping_quality',
                        'bases_number',
                        'contigs_number'
                        )
    ## FIXME Change how Charon stores these things? A dict for each attribute seems a little funky
    for field in fields_to_update:
        if not num_lanes:
            seqrun_dict[field] = {current_lane : lane_alignment_metrics[field]}
            seqrun_dict["mean_autosomal_coverage"] = 0
        else:
            seqrun_dict[field][current_lane] =  lane_alignment_metrics[field]
    seqrun_dict["mean_autosomal_coverage"] = seqrun_dict.get("mean_autosomal_coverage", 0) + lane_alignment_metrics["mean_autosomal_coverage"]


## TODO This can be moved to a more generic local_process_tracking submodule
def record_process_seqrun(project, sample, libprep, seqrun, workflow_subtask,
                          analysis_module_name, analysis_dir, slurm_job_id):
    LOG.info('Recording slurm job id "{}" for project "{}", sample "{}", libprep "{}", '
             'seqrun "{}", workflow "{}"'.format(slurm_job_id, project, sample, libprep,
                                                 seqrun, workflow_subtask))
    with get_db_session() as session:
        seqrun_db_obj = SeqrunAnalysis(project_id=project.project_id,
                                       project_name=project.name,
                                       project_base_path=project.base_path,
                                       sample_id=sample.name,
                                       libprep_id=libprep.name,
                                       seqrun_id=seqrun.name,
                                       engine=analysis_module_name,
                                       workflow=workflow_subtask,
                                       analysis_dir=analysis_dir,
                                       slurm_job_id=slurm_job_id)
        ## FIXME We must make sure that an entry for this doesn't already exist!
        session.add(seqrun_db_obj)
        for attempts in range(3):
            try:
                session.commit()
                LOG.info('Successfully recorded slurm job ID "{}" for project "{}", sample "{}", '
                         'libprep "{}", seqrun "{}", workflow "{}"'.format(slurm_job_id,
                                                                           project,
                                                                           sample,
                                                                           libprep,
                                                                           seqrun,
                                                                           workflow_subtask))
                break
            except sqlalchemy.exc.OperationalError:
                LOG.warn("Database is locked. Waiting...")
                time.sleep(15)
        else:
            raise RuntimeError('Could not record slurm job id "{}" for project "{}", sample "{}", '
                               'libprep "{}", seqrun "{}", workflow "{}"'.format(slurm_job_id,
                                                                                 project,
                                                                                 sample,
                                                                                 libprep,
                                                                                 seqrun,
                                                                                 workflow_subtask))
    try:
        LOG.info(('Updating Charon status for project/sample/libprep/seqrun '
                  '{}/{}/{}/{} to "RUNNING"').format(project, sample, libprep, seqrun))
        CharonSession().seqrun_update(projectid=project.project_id,
                                      sampleid=sample.name,
                                      libprepid=libprep.name,
                                      seqrunid=seqrun.name,
                                      alignment_status="RUNNING")
    except CharonError as e:
        LOG.warn('Could not update Charon status for project/sample/libprep/seqrun '
                 '{}/{}/{}/{} due to error: {}'.format(project, sample, libprep, seqrun, e))


## TODO This can be moved to a more generic local_process_tracking submodule
# FIXME change to use strings maybe
def record_process_sample(project, sample, workflow_subtask, analysis_module_name,
                          analysis_dir, slurm_job_id, config=None):
    LOG.info('Recording slurm job id "{}" for project "{}", sample "{}", '
             'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
    with get_db_session() as session:
        seqrun_db_obj = SampleAnalysis(project_id=project.project_id,
                                       project_name=project.name,
                                       project_base_path=project.base_path,
                                       sample_id=sample.name,
                                       engine=analysis_module_name,
                                       workflow=workflow_subtask,
                                       analysis_dir=analysis_dir,
                                       slurm_job_id=slurm_job_id)
        ## FIXME We must make sure that an entry for this doesn't already exist!
        session.add(seqrun_db_obj)
        for attempts in range(3):
            try:
                session.commit()
                LOG.info('Successfully recorded slurm job id "{}" for project "{}", sample "{}", '
                         'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
                break
            except sqlalchemy.exc.OperationalError:
                LOG.warn("Database locked. Waiting...")
                time.sleep(15)
        else:
            raise RuntimeError('Could not record slurm job id "{}" for project "{}", sample "{}", '
                               'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
    try:
        LOG.info(('Updating Charon status for project/sample '
                  '{}/{} to "RUNNING"').format(project, sample))
        CharonSession().seqrun_update(projectid=project.project_id,
                                      sampleid=sample.name,
                                      status="RUNNING")
    except CharonError as e:
        LOG.warn('Could not update Charon status for project/sample '
                 '{}/{} due to error: {}'.format(project, sample, e))


# Do we need this function?
def is_seqrun_analysis_running_local(workflow_subtask, project_id, sample_id,
                                     libprep_id, seqrun_id):
    """Determine if a flowcell is currently being analyzed by accessing the local
    process tracking database.

    :returns: True if under analysis, False otherwise
    """
    sequencing_run = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
    LOG.info('Checking if sequencing run "{}" is currently '
             'being analyzed (workflow "{}")...'.format(sequencing_run,
                                                        workflow_subtask))
    with get_db_session() as session:
        db_q = session.query(SeqrunAnalysis).filter_by(workflow=workflow_subtask,
                                                       project_id=project_id,
                                                       sample_id=sample_id,
                                                       libprep_id=libprep_id,
                                                       seqrun_id=seqrun_id)
        if session.query(db_q.exists()).scalar():
            LOG.info('...sequencing run "{}" is currently being analyzed.'.format(sequencing_run))
            return True
        else:
            LOG.info('...sequencing run "{}" is not currently under analysis.'.format(sequencing_run))
            return False


# Do we need this function?
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
            exit_code = f.read().strip()
            if exit_code:
                exit_code = int(exit_code)
            return exit_code
    except IOError as e:
        if e.errno == 2:    # No such file or directory
            return None     # Process is not yet complete
    except ValueError as e:
        raise ValueError('Could not determine job exit status: not an integer ("{}")'.format(e))
