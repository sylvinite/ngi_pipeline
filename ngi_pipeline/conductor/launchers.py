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
## TODO set so that if no projects are passed it just grabs all the ones from
##      Charon that are NEW or whatever
def launch_analysis_for_seqruns(projects_to_analyze, restart_failed_jobs=False,
                                config=None, config_file_path=None):
    """Launch the appropriate seqrun-level analysis for each fastq file in the project.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param bool restart_failed_jobs: Restart jobs marked as "FAILED" in Charon
    :param dict config: The parsed NGI configuration file; optional/has default.
    :param str config_file_path: The path to the NGI configuration file; optional/has default.
    """
    launch_analysis(level="seqrun", projects_to_analyze=projects_to_analyze,
                    restart_failed_jobs=restart_failed_jobs, config=config,
                    config_file_path=config_file_path)


## TODO add a "restart_running_jobs" parameter as well
## TODO set so that if no projects are passed it just grabs all the ones from
##      Charon that are ready for analysis I guess?
def launch_analysis_for_samples(projects_to_analyze, restart_failed_jobs=False,
                                config=None, config_file_path=None):
    """Launch the appropriate sample-level analysis for each sample in the project
    that has completed all prerequisite (e.g. seqrun-level) steps.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param bool restart_failed_jobs: Restart jobs marked as "FAILED" in Charon
    :param dict config: The parsed NGI configuration file; optional/has default.
    :param str config_file_path: The path to the NGI configuration file; optional/has default.
    """
    launch_analysis(level="sample", projects_to_analyze=projects_to_analyze,
                    restart_failed_jobs=restart_failed_jobs, config=config,
                    config_file_path=config_file_path)

@with_ngi_config
def launch_analysis(level, projects_to_analyze, restart_failed_jobs=False,
                    config=None, config_file_path=None):
    """Launch the appropriate seqrun (flowcell-level) analysis for each fastq
    file in the project.

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

        # This is weird
        objects_to_process = []
        if level == "sample":
            for sample in project:
                objects_to_process.append({"project": project, "sample": sample})
        elif level == "seqrun":
            for sample in project:
                for libprep in sample:
                    for seqrun in libprep:
                        objects_to_process.append({"project": project,
                                                   "sample": sample,
                                                   "libprep": libprep,
                                                   "seqrun": seqrun})
        # Still weird and not so great
        import pdb
        pdb.set_trace()
        for obj_dict in objects_to_process:
            project = obj_dict.get("project")
            sample = obj_dict.get("sample")
            libprep = obj_dict.get("libprep")
            seqrun = obj_dict.get("seqrun")

            try:
                if level == "seqrun":
                    charon_reported_status = charon_session.seqrun_get(project.project_id,
                                                                       sample, libprep,
                                                                       seqrun)['alignment_status']
                else: # sample-level
                    charon_reported_status = charon_session.sample_get(project.project_id,
                                                                       sample)['status']
            except (CharonError, KeyError) as e:
                LOG.error('Unable to get required information from Charon for '
                          'sample "{}" / project "{}" -- skipping: {}'.format(sample, project, e))
                continue
            # Check Charon to ensure this hasn't already been processed
            if charon_reported_status in ("RUNNING", "DONE"):
                LOG.info('Charon reports seqrun analysis for project "{}" / sample "{}" '
                         '/ libprep "{}" / seqrun "{}" does not need processing '
                         ' (already "{}")'.format(project, sample, libprep, seqrun,
                                                  charon_reported_status))
                continue
            elif charon_reported_status == "FAILED":
                if not restart_failed_jobs:
                    ## TODO change log messages
                    LOG.error('FAILED:  Project "{}" / sample "{}" / library "{}" '
                              '/ flowcell "{}": Charon reports FAILURE, manual '
                              'investigation needed!'.format(project, sample, libprep, seqrun))
                    continue
            try:
                # The engines themselves know which sub-workflows
                # they need to execute for a given level. For example,
                # with DNA Variant Calling on the sequencing run
                # level, we need to execute basic alignment and QC.
                if level == "seqrun":
                    LOG.info('Attempting to launch seqrun analysis for '
                             'project "{}" / sample "{}" / libprep "{}" '
                             '/ seqrun "{}", workflow "{}"'.format(project,
                                                                   sample,
                                                                   libprep,
                                                                   seqrun,
                                                                   workflow))
                    analysis_module.analyze_seqrun(project=project,
                                                   sample=sample,
                                                   libprep=libprep,
                                                   seqrun=seqrun)
                else: # sample level
                    LOG.info('Attempting to launch sample analysis for '
                             'project "{}" / sample {} / workflow '
                             '"{}"'.format(project, sample, libprep, seqrun, workflow))
                    analysis_module.analyze_sample(project=project,
                                                   sample=sample)

            except Exception as e:
                raise
                LOG.error('Cannot process project "{}" / sample "{}" / '
                          'libprep "{}" / seqrun "{}" / workflow '
                          '"{}" : {}'.format(project, sample, libprep,
                                             seqrun, workflow, e))
                set_new_seqrun_status = "FAILED"
                continue
