#!/usr/bin/env python

from __future__ import print_function

LOG = minimal_logger(__name__)


## NOTE This will be the function that is called by the Workflow Watcher script, or whatever we want to call it.
##      This is the part responsable to start flocell-level analysis, i.e., those analysis that can be run on each
##      project and each sample soon after they are sequenced.
##
##      This function should be called only by process_demultipled_flowcells, or by a part of the Helper to automatically
##      start analysis at flowcell level.
##      Check the ngi_pipeline_dummy_start.py to have an idea on how
##      the process will work.
##      In this context projects_to_analyse contains only the projects (and hence samples) present in the currently
##      under-investigation flowcell.
##
##
@with_ngi_config
def launch_analysis_for_flowcells(projects_to_analyze, restrict_to_samples=None, config=None, config_file_path=None):
    """Launch the analysis for fc-run, i.e., launch the correct analysis for each sample of each project 
    contained in the current flwcell(s).

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param list restrict_to_samples: A list of sample names to which we will restrict our analysis
    :param dict config: The parsed NGI configuration file; optional.
    :param list config_file_path: The path to the NGI configuration file; optional.
    """
    for project in projects_to_analyze:
        # Get information from the database regarding which workflows to run
        try:
            workflow = get_workflow_for_project(project.project_id)
        except (ValueError, IOError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project, e))
            LOG.error(error_msg)
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
                         "in project {}. Skipping.".format(analysis_module,
                                                           workflow,
                                                           project))
            LOG.error(error_msg)
            continue

        for sample in project.samples.values():
            for libprep in sample:
                for fcid in libprep:
                    #check that the current FlowCell is not already being analysed
                    #TODO: check also charon status here?
                    analysis_running = check_if_flowcell_analysis_are_running(project,
                        sample, libprep, fcid, config)
                    #if I am not running nothing on this run then I can start to analyse it
                    # IMPORTANT I know that this is a run so I need to start run specific analysis
                    # another function will take care of project specific analysis
                    if not analysis_running: #if this flowcell run is not already being analysed
                        try:
                            workflow = "dna_alignonly"  #must be taken from somewhere, either config file or Charon
                            #when I call an Engine at flowcell level I expect that the engine starts by defining its own
                            #folder structure and subsequently start analysis at flowcell level.
                            p_handle = analysis_module.analyze_flowcell_run(project=project,
                                                       sample= sample,
                                                       libprep = libprep,
                                                       fcid = fcid,
                                                       workflow_name=workflow,
                                                       config_file_path=config_file_path)

                            record_process_flowcell(p_handle, workflow, project,
                             sample, libprep, fcid, analysis_module, project.analysis_dir, config)

                        except Exception as e:
                            error_msg = ('Cannot process project "{}": {}'.format(project, e))
                            LOG.error(error_msg)
                            continue

## NOTE This will be the function that is called by the Workflow Watcher script, or whatever we want to call it
##      By this I mean the script that checks intermittently to determine if we can move on with the next workflow,
##      whether this is something periodic (like a cron job) or something triggered by the completion of another part
##      of the code (event-based, i.e. via Celery)
##
##      At the moment it requires a list of projects to analyze, which suggests it is called by another
##      function that has just finished doing something with those project (i.e. the Celery approach);
##      if it is to be called periodically, I would suggest that the periodic calling function
##      (i.e. whatever script the cron job calls) uses another function that goes through the database
##      and finds all Projects for which the "Status" is not "Complete" or something to that effect,
##      and then hands that list to this function.
#def launch_analysis_for_projects(projects_to_analyze, restrict_to_samples=None, config_file_path=None):
#    """Launch the analysis of projects.
#
#    :param list projects_to_analyze: The list of projects (Project objects) to analyze
#    :param list restrict_to_samples: A list of sample names to which we will restrict our analysis
#    :param list config_file_path: The path to the NGI Pipeline configuration file.
#    """
#
#    if not config_file_path:
#        config_file_path = locate_ngi_config()
#    config = load_yaml_config(config_file_path)
#    for project in projects_to_analyze:
#        # Get information from the database regarding which workflows to run
#        try:
#            workflow = get_workflow_for_project(project.project_id)
#        except (ValueError, IOError) as e:
#            error_msg = ("Skipping project {} because of error: {}".format(project, e))
#            LOG.error(error_msg)
#            continue
#        try:
#            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
#        except KeyError:
#            error_msg = ("No analysis engine for workflow \"{}\" specified "
#                         "in configuration file. Skipping this workflow "
#                         "for project {}".format(workflow, project))
#            LOG.error(error_msg)
#            raise RuntimeError(error_msg)
#        # Import the adapter module specified in the config file (e.g. piper_ngi)
#        try:
#            analysis_module = importlib.import_module(analysis_engine_module_name)
#        except ImportError as e:
#            error_msg = ("Couldn't import module {} for workflow {} "
#                         "in project {}. Skipping.".format(analysis_module,
#                                                           workflow,
#                                                           project))
#            LOG.error(error_msg)
#            continue
#        try:
#            #this happens at project level butI need to track actions at Samples level!!!!
#            p_handle = analysis_module.analyze_project(project=project,
#                                                       workflow_name=workflow,
#                                                       config_file_path=config_file_path)
#
#            #this must be tracked at run level
#            # For now only tracking this on the project level
#            record_workflow_process_local(p_handle, workflow, project, analysis_module, config)
#        except Exception as e:
#            error_msg = ('Cannot process project "{}": {}'.format(project, e))
#            LOG.error(error_msg)
#            continue



## NOTE
## This function is responsable of trigger second level analyisis (i.e., sample level analysis)
## using the information available on the Charon.
## TOO MANY CALLS TO CHARON ARE MADE HERE: we need to restrict them
@with_ngi_config
def trigger_sample_level_analysis(config=None, config_file_path=None):
    """Triggers secondary analysis based on what is found on Charon
    for now this will work only with Piper/IGN

    :param dict config: The parsed NGI configuration file; optional.
    :param list config_file_path: The path to the NGI configuration file; optional.
    """
    #start by getting all projects, this will likely need a specific API
    charon_session = get_charon_session()
    url = construct_charon_url("projects")
    projects_response = charon_session.get(url)
    if projects_response.status_code != 200:
        error_msg = ('Error accessing database: could not get all projects: {}'.format(project_response.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)

    projects_dict = projects_response.json()["projects"]

    for project in projects_dict:
        #check if the field Pipeline is set
        project_id = project["projectid"]

        try:
            workflow = get_workflow_for_project(project_id)
        except (RuntimeError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project_id, e))
            LOG.error(error_msg)
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
                         "in project {}. Skipping.".format(analysis_module,
                                                            workflow,
                                                            project_id))
            LOG.error(error_msg)
            continue


        #I know which engine I need to use to process sample ready, however only the engine
        #knows that are the conditions that need to be made
        LOG.info('Checking for ready to be analysed samples in project {} with workflow {}'.format(project_id, workflow))
        #get all the samples from Charon
        url = construct_charon_url("samples", project_id)
        samples_response = charon_session.get(url)
        if samples_response.status_code != 200:
            error_msg = ('Error accessing database: could not get samples for projects: {}'.format(project_id,
                            project_response.reason))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        samples_dict = samples_response.json()["samples"]
        #now recreacte the project object
        analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
        proj_dir = os.path.join(analysis_top_dir, "DATA", project["name"])
        projectObj = createIGNproject(analysis_top_dir, project["name"],  project_id)

        analysis_dir = os.path.join(analysis_top_dir, "ANALYSIS", project["name"] )

        for sample in samples_dict: #sample_dict is a charon object
            sample_id = sample["sampleid"]
            #check that it is not already running
            analysis_running = check_if_sample_analysis_are_running(projectObj, projectObj.samples[sample_id], config)
            #check that this analysis is not already done
            if "status" in sample and sample["status"] == "done":
                analysis_done = True
            else:
                analysis_done = False

            if not analysis_running and not analysis_done: #I need to avoid start process if things are done
                try:
                    # note here I do not know if I am going to start some anlaysis or not, depends on the Engine that is called
                    #I am here even with project that have no analysis ... maybe better to define a flag?
                    p_handle = analysis_module.analyse_sample_run(sample = sample , project = projectObj,
                                                              config_file_path=config_file_path )
                    #p_handle is None when the engine decided that there is nothing to be done
                    if p_handle != 1:
                        record_process_sample(p_handle, workflow, projectObj, sample_id, analysis_module,
                            analysis_dir, config)
                except Exception as e:
                    error_msg = ('Cannot process sample {} in project {}: {}'.format(sample_id, project_id, e))
                    LOG.error(error_msg)
                    continue
            elif analysis_done:
                LOG.info("Project {}, Sample {}  "
                     "have been succesfully processed.".format(project_id, sample_id))





