#!/usr/bin/env python

from __future__ import print_function

import importlib
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.filesystem import recreate_project_from_db
from ngi_pipeline.piper_ngi.local_process_tracking import update_charon_with_local_jobs_status
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config


LOG = minimal_logger(__name__)


# This flowcell-level analysis function is called automatically after newly-delivered
# flowcells are reorganized into projects. It runs only at the highest "flowcell" 
# or "sequencing run" level, e.g. individual fastq files with none of their
# relationships considered (i.e. two fastq files from the same sample are analyzed
# independently).
@with_ngi_config
## TODO change name to trigger_seqrun_analysis or something
def launch_analysis_for_flowcells(projects_to_analyze, restart_failed_jobs=False,
                                  config=None, config_file_path=None):
    """Launch the appropriate flowcell-level analysis for each fastq file in the project.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param dict config: The parsed NGI configuration file; optional/has default.
    :param str config_file_path: The path to the NGI configuration file; optional/has default.
    """
    # Update Charon with the local state of all the jobs we're running
    update_charon_with_local_jobs_status()
    charon_session = CharonSession()
    for project in projects_to_analyze:
        # Get information from Charon regarding which workflows to run
        try:
            # E.g. "NGI" for NGI DNA Samples
            workflow = charon_session.project_get(project.project_id)["pipeline"]
        except (KeyError, CharonError) as e:
            # Workflow missing from Charon?
            LOG.error('Skipping project "{}" because of error: {}'.format(project, e))
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
            error_msg = ('Skipping project "{}" workflow "{}": couldn\'t import '
                         'module "{}": {}'.format(project, workflow, analysis_engine_module_name, e))
            LOG.error(error_msg)
            # Next project
            continue

        # Get all the sequencing runs for this project from Charon
        for sample in project:
            for libprep in sample:
                for seqrun in libprep:
                    # Check Charon to ensure this hasn't already been processed
                    charon_reported_status = charon_session.seqrun_get(project.project_id,
                                                                       sample, libprep,
                                                                       seqrun)['alignment_status']
                    if charon_reported_status in ("RUNNING", "DONE"):
                        #LOG.info("<already running>")
                        continue
                    elif charon_reported_status == "FAILED":
                        if not restart_failed_jobs:
                            LOG.error('FAILED:  Project "{}" / sample "{}" / library "{}" '
                                      '/ flowcell "{}": Charon reports FAILURE, manual '
                                      'investigation needed!'.format(project, sample, libprep, seqrun))
                            continue
                    try:
                        # The engines themselves know which sub-workflows
                        # they need to execute for a given level. For example,
                        # with DNA Variant Calling on the sequencing run
                        # level, we need to execute basic alignment and QC.
                        LOG.info('Attempting to launch seqrun analysis for project "{}" / sample "{}" / '
                                 'libprep "{}" / seqrun "{}", workflow "{}"'.format(project, sample, libprep, seqrun, workflow))
                        analysis_module.analyze_seqrun(project=project,
                                                       sample=sample,
                                                       libprep=libprep,
                                                       seqrun=seqrun)

                    except Exception as e:
                        LOG.error('Cannot process project "{}" / sample "{}" / '
                                  'libprep "{}" / seqrun "{}" / workflow '
                                  '"{}" : {}'.format(project, sample, libprep,
                                                     seqrun, workflow, e))
                        set_new_seqrun_status = "FAILED"
                        continue


@with_ngi_config
def trigger_sample_level_analysis(restrict_to_projects=None, restrict_to_samples=None, config=None, config_file_path=None):
    """Triggers secondary analysis based on what is found on Charon
    for now this will work only with Piper/IGN

    :param dict config: The parsed NGI configuration file; optional.
    :param list config_file_path: The path to the NGI configuration file; optional.
    """
    # Update all jobs status first
    update_charon_with_local_jobs_status()
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
