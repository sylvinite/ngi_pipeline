#!/usr/bin/env python

from __future__ import print_function

import importlib
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.filesystem import recreate_project_from_db
from ngi_pipeline.database.communicate import get_workflow_for_project
## YOU'RE NEXT
from ngi_pipeline.database.local_process_tracking import check_update_jobs_status
from ngi_pipeline.piper_ngi.local_process_tracking import is_seqrun_analysis_running_local, \
                                                          is_sample_analysis_running_local, \
                                                          record_process_seqrun, \
                                                          record_process_sample
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config


LOG = minimal_logger(__name__)


# This flowcell-level analysis function is called automatically after newly-delivered flowcells are reorganized
# into projects. It runs only at the highest "flowcell" or "sequencing run" level, e.g. individual fastq files
# with none of their relationships considered (i.e. two fastq files from the same sample are analyzed independently).
@with_ngi_config
## TODO change name to trigger_seqrun_analysis or something
def launch_analysis_for_flowcells(projects_to_analyze, config=None, config_file_path=None):
    """Launch the appropriate flowcell-level analysis for each fastq file in the project.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param dict config: The parsed NGI configuration file; optional/has default.
    :param str config_file_path: The path to the NGI configuration file; optional/has default.
    """
    for project in projects_to_analyze:
        # Get information from Charon regarding which workflows to run
        try:
            workflow = get_workflow_for_project(project.project_id)
        except (ValueError, CharonError) as e:
            LOG.error("Skipping project {} because of error: {}".format(project, e))
            continue
        try:
            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
        except KeyError:
            error_msg = ("No analysis engine for workflow \"{}\" specified "
                         "in configuration file. Skipping this workflow "
                         "for project {}".format(workflow, project))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        # Import the adapter module specified in the config file (e.g. piper_ngi)
        try:
            analysis_module = importlib.import_module(analysis_engine_module_name)
        except ImportError as e:
            error_msg = ("Couldn't import module {} for workflow {} "
                         "in project {}: \"{}\". Skipping.".format(analysis_engine_module_name,
                                                                   workflow, project, e))
            LOG.error(error_msg)
            # Next project
            continue

        charon_session = CharonSession()
        for sample in project:
            for libprep in sample:
                for seqrun in libprep:
                    # Check Charon to ensure this hasn't already been processed
                    charon_reported_status = charon_session.seqrun_get(project.project_id, sample, libprep, seqrun).get('alignment_status')
                    if charon_reported_status and charon_reported_status in ("RUNNING", "DONE"):
                        # If charon_reported_status is  RUNNING or DONE skip processing but check that logic is respected (spok roks!!!)
                        if charon_reported_status == "RUNNING":
                            if not is_seqrun_analysis_running_local(workflow=workflow,
                                                                    project=project,
                                                                    sample=sample,
                                                                    libprep=libprep,
                                                                    seqrun=seqrun):
                                error_msg = ('Charon and local db incongruency: project "{}" / sample "{}" / library "{}" / flowcell "{}": '
                                             'Charon reports it as running but not trace of it in local DB'.format(project, sample, libprep, seqrun))
                                LOG.error(error_msg)
                        else: #otherwise I am DONE
                            if is_seqrun_analysis_running_local(workflow=workflow,
                                                                project=project,
                                                                sample=sample,
                                                                libprep=libprep,
                                                                seqrun=seqrun):
                                error_msg = ('Charon and local db incongruency:  Project "{}", Sample "{}", Library "{}", flowcell "{}": '
                                             'Charon reports it is DONE but local db says it is RUNNING'.format(project, sample, libprep, seqrun))
                                LOG.error(error_msg)

                        continue
                    # if i am here the charon_reported_status on charon is either None, NEW, or FAILED
                    # Check the local jobs database to determine if this flowcell is already being analyzed
                    set_new_seqrun_status = None
                    if charon_reported_status and charon_reported_status == "FAILED":
                        error_msg = ('FAILED:  Project "{}" / sample "{}" / library "{}" / flowcell "{}": '
                                     'Charon reports FAILURE, manual investigation needed!'.format(project, sample, libprep, seqrun))
                        ## TODO send an email or something, but that should be a Charon-related process, not done here
                        LOG.error(error_msg)
                        ### FIXME these next two line are a temp fix because Nestor is misbehaving and we just want to retry the analysis
                        ##        note that here it would be nice to differentiate between DATA_FAILURE and COMPUTE_FAILURE!!
                        LOG.warn("Continuing with project anyway (remove me later)")
                        #continue
                    # at this point the status is only None or NEW, I need only to check that the analysis is already running (which would be strange)
                    if not is_seqrun_analysis_running_local(workflow=workflow,
                                                            project=project,
                                                            sample=sample,
                                                            libprep=libprep,
                                                            seqrun=seqrun):
                        try:
                            # This workflow thing will be handled on the engine side. Here we'll just call like "piper_ngi.flowcell_level_analysis"
                            # or something and it will handle which workflows to execute (qc, alignment, ...)
                            workflow_name = "dna_alignonly"  #must be taken from somewhere, either config file or Charon

                            # Here we are not specifying any kind of output directory as I believe this will be pulled from
                            # the config file; however, we may have to adapt this as we add more engines.

                            ## TODO I think we need to detach these sessions or something as they will die
                            ##      when the main Python thread dies; however, this means ctrl-c will not kill them.
                            ##      This is probably alright as this will generally be run automatically.
                            LOG.info('Attempting to launch flowcell analysis for project "{}" / sample "{}" / '
                                     'libprep "{}" / seqrun "{}", workflow "{}"'.format(project, sample, libprep, seqrun, workflow))
                            p_handle = analysis_module.analyze_flowcell_run(project=project,
                                                                            sample=sample,
                                                                            libprep=libprep,
                                                                            seqrun=seqrun,
                                                                            workflow_name=workflow_name)

                            ## NOTE what happens when the process fails? we still get a Popen object I think?
                            if p_handle:
                                LOG.info("...success! Attempting to record job in local jobs database. Hope this works!")
                                record_process_seqrun(project=project,
                                                      sample=sample,
                                                      libprep=libprep,
                                                      seqrun=seqrun,
                                                      workflow_name=workflow_name,
                                                      analysis_module_name=analysis_module.__name__,
                                                      analysis_dir=project.analysis_dir,
                                                      pid=p_handle.pid)
                                LOG.info("...success!")
                                set_new_seqrun_status = "RUNNING"
                            else:
                                LOG.error("...failed! Retry again later or something")
                        # TODO which exceptions can we expect to be raised here?
                        except Exception as e:
                            LOG.error('Cannot process project "{}" / sample "{}" / libprep "{}" / '
                                      'seqrun "{}": {}'.format(project, sample, libprep, seqrun, e))
                            set_new_seqrun_status = "FAILED"
                            continue
                    else:
                        error_msg = ('Charon and local db incongruency: Skipping analysis of project "{}" / '
                                     'sample "{}" / library "{}" / seqrun "{}": Charon reports it is "{}" '
                                     'but local db says it is running'.format(project, sample, libprep, seqrun, charon_reported_status))
                        LOG.error(error_msg)
                        continue
                    if set_new_seqrun_status:
                        try:
                           LOG.info('Updating Charon entry for project "{}" / sample "{}" / libprep "{}" / '
                                    'seqrun "{}" to "{}"...'.format(project, sample, libprep, seqrun, set_new_seqrun_status))
                           charon_session.seqrun_update(projectid=project, sampleid=sample,
                                                        libprepid=libprep, seqrunid=seqrun,
                                                        alignment_status=set_new_seqrun_status)
                           LOG.info("...success.")
                        except CharonError as e:
                           LOG.error("...could not update Charon!: {}".format(e))


@with_ngi_config
def trigger_sample_level_analysis(restrict_to_projects=None, restrict_to_samples=None, config=None, config_file_path=None):
    """Triggers secondary analysis based on what is found on Charon
    for now this will work only with Piper/IGN

    :param dict config: The parsed NGI configuration file; optional.
    :param list config_file_path: The path to the NGI configuration file; optional.
    """
    # Update all jobs status first
    check_update_jobs_status()
    LOG.info("Starting sample-level analysis routine.")
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    charon_session = CharonSession()
    try:
        ## TODO it would be nice here if we had Charon just give us all projects matching a certain set of criteria
        ##      eventually there will be too many projects to just grab them all
        projects_dict = charon_session.projects_get_all()['projects']
    except (CharonError, KeyError) as e:
        raise RuntimeError("Unable to get list of projects from Charon and cannot continue: {}".format(e))
    for project in projects_dict:
        if restrict_to_projects and project.get('name') not in restrict_to_projects and \
                                    project.get('projectid') not in restrict_to_projects:
            LOG.debug('Skipping project "{}": not in specified list of projects ' \
                     '({})'.format(project.get('name'), ", ".join(restrict_to_projects)))
            continue
        if project.get("status") in ("CLOSED", "ABORTED"):
            LOG.info("Skipping project {}: marked as {}".format(project, project.get("status")))
        project_id = project.get("projectid")
        try:
            workflow = get_workflow_for_project(project_id)
        except (RuntimeError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project_id, e))
            LOG.error(error_msg)
            # Next project
            continue
        try:
            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
        except KeyError:
            error_msg = ("No analysis engine for workflow \"{}\" specified "
                         "in configuration file. Skipping this workflow "
                         "for project {}".format(workflow, project))
            #LOG.error(error_msg)
            raise RuntimeError(error_msg)
        # Import the adapter module specified in the config file (e.g. piper_ngi)
        try:
            analysis_module = importlib.import_module(analysis_engine_module_name)
        except ImportError as e:
            error_msg = ("Couldn't import module {} for workflow {} "
                         "in project {}. Skipping.".format(analysis_module,
                                                            workflow,
                                                            project_id))
            LOG.error(error_msg)
            continue
        analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
        proj_dir = os.path.join(analysis_top_dir, "DATA", project["name"])
        try:
            # Now recreate the project object
            ## NOTE this actually adds a "status" attribute to each object it recreates using info from Charon.
            ##      It's useful in this specific case but feels a little funky.
            ##      May rework this when restructuring the code later.
            project_obj = recreate_project_from_db(analysis_top_dir, project["name"],  project_id)
        except RuntimeError as e:
            LOG.error("Skipping project {}: could not recreate object from Charon: {}".format(project_id, e))
            continue
        analysis_dir = os.path.join(analysis_top_dir, "ANALYSIS", project["name"] )

        #I know which engine I need to use to process sample ready, however only the engine
        #knows that are the conditions that need to be made
        LOG.info('Finding samples read to be analyzed in project {} / workflow {}'.format(project_id, workflow))
        try:
            samples_dict = charon_session.project_get_samples(project_id)["samples"]
        except CharonError as e:
            raise RuntimeError('"Could not access samples for project "{}": "{}"'.format(project_id, e))

        for sample_obj in project_obj: #sample_dict is a charon object
            if restrict_to_samples and sample_obj.name in restrict_to_samples:
                LOG.debug('Skipping sample "{}": not in specified list of samples ({})'.format(", ".join(restrict_to_samples)))
                continue
            # This status comes from the Charon database
            if sample_obj.status in ("DONE", "COMPUTATION_FAILED", "DATA_FAILED", "IGNORE"):
                LOG.info('Sample "{}" in project "{}" will not be processed at this time: '
                         'status is "{}".'.format(sample_obj, project_obj, sample_obj.status))
            # Checks the local job-tracking database to determine if this sample analysis is currently ongoing
            elif not is_sample_analysis_running_local(workflow=workflow,
                                                      project=project_obj,
                                                      sample=sample_obj):
                # Analysis not marked as "DONE" in Charon and also not yet running locally -- needs to be launched
                ## FIXME Mario this needs to be engine-agnostic
                try:
                    p_handle = analysis_module.analyze_sample_run(project=project_obj,
                                                                  sample=sample_obj)
                except RuntimeError as e:
                    # Some problem launching the job
                    error_msg = ('Cannot process sample "{}" in project "{}": {}'.format(sample_obj, project_obj, e))
                    LOG.error(error_msg)
                    continue
                if p_handle:    # p_handle is None when the engine decided that there is nothing to be done
                    record_process_sample(project=project_obj,
                                          sample=sample_obj,
                                          workflow_name=workflow,
                                          analysis_module_name=analysis_module.__name__(),
                                          analysis_dir=project.analysis_dir,
                                          pid=p_handle.pid,
                                          config=config)
                    #record_process_sample(p_handle, workflow, project_obj, sample_obj,
                    #                      analysis_module, analysis_dir, config)
                else:
                    raise Exception("Aaaaagggrhghrhgrhghgh")
