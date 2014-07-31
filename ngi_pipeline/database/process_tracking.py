"""Keeps track of running workflow processes"""
import json
import shelve
import os
import glob
import re

from ngi_pipeline.database import construct_charon_url, get_charon_session
from ngi_pipeline.log import minimal_logger
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from ngi_pipeline.database import get_project_id_from_name
from ngi_pipeline.utils.parsers import parse_genome_results

LOG = minimal_logger(__name__)


def get_all_tracked_processes(config=None):
    """Returns all the processes that are being tracked locally,
    which is to say all the processes that have a record in our local
    process_tracking database.

    :param dict config: The parsed configuration file (optional)

    :returns: The dict of the entire database
    :rtype: dict
    """
    # This function doesn't do a whole lot
    db = get_shelve_database(config)
    db_dict = {}
    for job_name, job_name_obj in db.iteritems():
        db_dict[job_name] = job_name_obj
    db.close()
    return db_dict


def remove_record_from_local_tracking(project, config=None):
    """Remove a record from the local tracking database.

    :param NGIProject project: The NGIProject object
    :param dict config: The parsed configuration file (optional)

    :raises RuntimeError: If the record could not be deleted
    """
    LOG.info('Attempting to remove local process record for '
             'project "{}"'.format(project))
    db = get_shelve_database(config)
    try:
        del db[project] #POP does not remove the entry in the db
    except KeyError:
        error_msg = ('Project "{}" not found in local process '
                     'tracking database.'.format(project))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    db.close()


def check_if_flowcell_analysis_are_running( project, sample, libprep, fcid, config=None):
    """checks if a given project/sample/library/flowcell is currently analysed.
    
    :param Project project: the project object
    :param Sample sample:
    :param LibPrep libprep:
    :param string fcid:
    :param dict config: The parsed configuration file (optional)

    :raises RuntimeError: If the configuration file cannot be found
    
    :returns true/false
    """
    LOG.info("checking if Project {}, Sample {}, Library prep {}, fcid {} "
             "are currently being analysed ".format(project, sample, libprep, fcid))
    db = get_shelve_database(config)
    if project.name in db:
        error_msg = ("Project {},is already being analysed at project level "
                     "This run should not exists!!! somethig terribly wrong is happening, "
                     "Kill everything  and call Mario and Francesco".format(project))
        LOG.info(error_msg)
        sys.exit("Quitting: " + error_msg)
        
    if "{}_{}".format(project.name, sample) in db:
        error_msg = ("Project {} and sample {}, is already being analysed at sample level "
                     "This run should not exists!!! somethig terribly wrong is happening, "
                     "Kill everything  and call Mario and Francesco".format(project, sample))
        LOG.info(error_msg)
        sys.exit("Quitting: " + error_msg)

    db_key = "{}_{}_{}_{}".format(project, sample, libprep, fcid)
    if db_key in db:
        error_msg = ("Project {}, Sample {}, Library prep {}, fcid {} "
                     "has an entry in the local db. Skipping this analys."
                     "rhis should not happen".format(project, sample, libprep, fcid))
        LOG.warn(error_msg)
        db.close()
        return True
    db.close()
    return False





def write_status_to_charon(project_id, return_code):
    """Update the status of a workflow for a project in the Charon database.

    :param NGIProject project_id: The name of the project
    :param int return_code: The return code of the workflow process

    :raises RuntimeError: If the Charon database could not be updated
    """
    
    charon_session = get_charon_session()
    status = "Completed" if return_code is 0 else "Failed"
    project_url = construct_charon_url("project", project_id)
    project_response = charon_session.get(project_url)
    if project_response.status_code != 200:
        error_msg = ('Error accessing database for project "{}"; could not '
                     'update Charon: {}'.format(project_id, project_response.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    project_dict = project_response.json()
    #project_dict["status"] = status
    response_obj = charon_session.put(project_url, json.dumps(project_dict))
    if response_obj.status_code != 204:
        error_msg = ('Failed to update project status for "{}" '
                     'in Charon database: {}'.format(project_id, response_obj.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)

def write_to_charon_alignment_results(job_id, return_code, run_dir=None):
    """Update the status of a sequencing run after alignment.

    :param NGIProject project_id: The name of the project, sample, lib prep, flowcell id
    :param int return_code: The return code of the workflow process
    :param string run_dir: the directory where results are stored (I know that I am running piper)

    :raises RuntimeError: If the Charon database could not be updated
    """
    
    charon_session = get_charon_session()
    alignment_status = "Done" if return_code is 0 else "Aborted"
    

    ## A.Wedell_13_03_P567_102_A_130627_AH0JYUADXX
    information_to_extract = re.compile("([a-zA-Z]\.[a-zA-Z]*_\d*_\d*)_(P\d*_\d*)_([A-Z])_(\d{6})_(.*)")
    project_name = information_to_extract.match(job_id).group(1)
    project_id   = get_project_id_from_name(project_name)
    sample_id    = information_to_extract.match(job_id).group(2)
    library_id   = information_to_extract.match(job_id).group(3)
    run_id_piper = information_to_extract.match(job_id).group(5)
    run_id       = "{}_{}".format(information_to_extract.match(job_id).group(4),
                                information_to_extract.match(job_id).group(5))

    #this returns url to all the seq runs of this library, sample, project
    url = construct_charon_url("seqruns", project_id, sample_id, library_id)
    #now i need to check with one is my run id, I need to match the runid field with my local run_id
    seqruns_response = charon_session.get(url)
    if seqruns_response.status_code != 200:
        error_msg = ('Error accessing database while updaiting alignment statistics'
                     'for project {} sample {} library {} run {}  could not '
                     'update Charon: {}'.format(project_id, sample_id, library_id,
                      run_id, project_response.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    #get all seqruns for this lib prep and selcet the one I have to update
    #Charon needs to allow this in a easyer way?
    seqruns_dict            = seqruns_response.json()
    seq_run_to_update       = -1
    seq_run_to_update_dict  = {}
    for seqrun in seqruns_dict["seqruns"]:
        run_id_charon         = seqrun["runid"]
        run_id_charon_extract = re.compile("(\d{6})_.*_.*_(.*)")
        #create the run id as it is stored in the jobid field
        run_id_to_check       = "{}_{}".format(run_id_charon_extract.match(run_id_charon).group(1),
                                    run_id_charon_extract.match(run_id_charon).group(2))
        if run_id_to_check == run_id:
            if seq_run_to_update > 0:
                error_msg = ('Error while trying to update seqrun {}. Found two hits on Charon'
                            'that match the same runid. this is not possible'.format(run_id_charon))
                LOG.error(error_msg)
                raise RunTimeError(error_msg)
            #otherwise I can update this entry
            seq_run_to_update       = seqrun["seqrunid"]
            seq_run_to_update_dict  = seqrun
        
    if seq_run_to_update == -1: #in this case I did not find a seqrun matching the one I want to update
        error_msg = ('Error while trying to update seqrun {}. No Hits found on Charon '
                            'that match the same runid. this is not possible'.format(run_id))
        LOG.error(error_msg)
        raise RunTimeError(error_msg)
 

    #At this point I can update the correct seqrun
    url = construct_charon_url("seqrun", project_id, sample_id, library_id, seq_run_to_update)
    all_algn_completed = True  # this variable is used to check that all alignments have a result
    #no I can start to put things
    if return_code is None:
        seq_run_to_update_dict["alignment_status"]="Working" #mark on charon that we are aligning this run
    elif return_code == 0:
        #in this case I need to update the alignment statistics for each lane in this seq run
        if "alignment_status" in seq_run_to_update_dict and seq_run_to_update_dict["alignment_status"] == "Done":
            warn_msg = ('Sequencing run {} already declered finished but now re-updaiting it '
                        'this will couse over-writing of all fields'.format(run_id))
            LOG.warn(warn_msg)
            #TODO: need to delete seq_rrun_to_update_dict
        
        seq_run_to_update_dict["lanes"] = 0 #set this to zero so I know that I can over-write the fields
        
        try:
            piper_result_dir = os.path.join(run_dir, "02_preliminary_alignment_qc")
        except:
            error_msg = ('Error while trying to update seqrun {}. No run_dir has been specified '
                          'for project {}, sample {}, libprep {}'.format(run_id, project_id, sample_id,
                          library_id))
            LOG.error(error_msg)
            raise RunTimeError(error_msg)
        if not os.path.isdir(piper_result_dir):
            #this should not happen,if return code is 0 this folder should exist but I want to check it anyway
            all_algn_completed =False
        else:
            piper_qc_dir_base = "{}.{}.{}".format(sample_id, run_id_piper, sample_id)
            piper_qc_path     = "{}*/".format(os.path.join(piper_result_dir, piper_qc_dir_base))
            for qc_dir in glob.glob(piper_qc_path): #for each lane
                genome_result = os.path.join(qc_dir, "genome_results.txt")
                if not os.path.isfile(genome_result):
                    #this should not happen,if return code is 0 this file should exist but I want to check it anyway
                    all_algn_completed =False
                else:
                    alignment_results = parse_genome_results(genome_result)
                    seq_run_to_update_dict = update_seq_run(seq_run_to_update_dict, alignment_results)
                    
        if not all_algn_completed:
            error_msg = ('Alignment ended with an error: Piper returned 0 as error code but file structure is unexpected'
                      'currently processing runid {} for project {}, sample {}, libprep {}'.format(run_id,
                      project_id, sample_id, library_id))
            LOG.error(error_msg)
            seq_run_to_update_dict["alignment_status"] = "Aborted"
        else:
            seq_run_to_update_dict["alignment_status"] = "Done"

    else:
        #Alignment failed, store it as aborted, I will not use this until it is not fixed
        error_msg = ('Alignment ended with an error: Piper returned non 0 return code'
                      'currently processing runid {} for project {}, sample {}, libprep {}'.format(run_id,
                      project_id, sample_id, library_id))
        LOG.error(error_msg)
        seq_run_to_update_dict["alignment_status"] = "Aborted"


    if seq_run_to_update_dict["alignment_status"] == "Aborted":
        #in such a case I do not update the other fields
        seq_run_to_update_dict = delete_seq_run_update(seq_run_to_update)

    response_obj = charon_session.put(url, json.dumps(seq_run_to_update_dict))


def update_seq_run(seq_run_to_update_dict, alignment_results):
    lanes = seq_run_to_update_dict["lanes"]
    seq_run_to_update_dict["lanes"] += 1;
    #get the current lane
    extractLane = re.compile(".*\.(\d)\.bam")
    current_lane = extractLane.match(alignment_results["bam_file"]).group(1)
    
    fields_to_update = ('mean_autosome_coverage',
                        'mean_coverage',
                        'std_coverage',
                        'aligned_bases',
                        'mapped_bases',
                        'mapped_reads',
                        'reads',
                        'sequenced_bases',
                        'bam_file',
                        'output_file',
                        'GC_percentage',
                        'mean_mapping_quality',
                        'bases_number',
                        'contigs_number'
                        )

    for field in fields_to_update:
        if field == 'mean_autosome_coverage':
            if lanes == 0:
                seq_run_to_update_dict[field] = alignment_results[field]
            else:
                seq_run_to_update_dict[field] += alignment_results[field]
        else:
            if lanes == 0:
                seq_run_to_update_dict[field] = "{}_{}".format(current_lane, alignment_results[field])
            else:
                seq_run_to_update_dict[field] += " ; {}_{}".format(current_lane, alignment_results[field])

    return seq_run_to_update_dict

def delete_seq_run_update(seq_run_to_update):
    fields_to_delete = ('mean_autosome_coverage',
                        'mean_coverage',
                        'std_coverage',
                        'aligned_bases',
                        'mapped_bases',
                        'mapped_reads',
                        'reads',
                        'sequenced_bases',
                        'bam_file',
                        'output_file',
                        'GC_percentage',
                        'mean_mapping_quality',
                        'bases_number',
                        'contigs_number'
                        )
    for field in fields_to_delete:
        if field in seq_run_to_update:
            del(seq_run_to_update[field])
    return seq_run_to_update




def record_workflow_process_local(p_handle, workflow, project, analysis_module, analysis_dir, config=None):
    """Track the PID for running workflow analysis processes.

    :param subprocess.Popen p_handle: The subprocess.Popen object which executed the command
    :param str workflow: The name of the workflow that is running
    :param Project project: The Project object for which the workflow is running
    :param analysis_module: The analysis module used to execute the workflow
    :param dict config: The parsed configuration file (optional)


     Stored dict resembles {"J.Doe_14_01":
                                {"workflow": "NGI",
                                 "p_handle": p_handle,
                                 "analysis_module": analysis_module.__name__,
                                 "project_id": project_id
                                 "run_dir" : running dir
                                }
                             "J.Johansson_14_02_P201_101_A_RUNID":
                                 ...
                            }

    

    :raises KeyError: If the database portion of the configuration file is missing
    :raises RuntimeError: If the configuration file cannot be found
    :raises ValueError: If the project already has an entry in the database.
    """
    ## Probably better to use an actual SQL database for this so we can
    ## filter by whatever -- project name, analysis module name, pid, etc.
    ## For the prototyping we can use shelve but later move to sqlite3 or sqlalchemy+Postgres/MySQL/whatever
    LOG.info("Recording process id {} for project {}, " 
             "workflow {}".format(p_handle.pid, project, workflow))
    project_dict = { "workflow": workflow,
                     "p_handle": p_handle,
                     "analysis_module": analysis_module.__name__,
                     "project_id": project.project_id,
                     "run_dir": analysis_dir
                   }
    db = get_shelve_database(config)
    # I don't see how this would ever happen but it makes me nervous to not
    # even check for this.
    #this will happen often.... we need to check that we are not rerunning the same analysis at the same moment
    if project.name in db:
        error_msg = ("Project {} already has an entry in the local process "
                     "tracking database -- this should not be. Overwriting!".format(project.name))
        LOG.warn(error_msg)
    db[project.name] = project_dict
    db.close()
    LOG.info("Successfully recorded process id {} for project {} (ID {}), " 
             "workflow {}".format(p_handle.pid, project, project.project_id, workflow))


def record_workflow_process_run_local(p_handle, workflow, project,
  sample, libprep, fcid, analysis_module, analysis_dir, config=None):
    LOG.info("Recording process id {} for project {}, sample {}, fcid {} "
             "workflow {}".format(p_handle.pid, project, sample, fcid, workflow))
    project_dict = { "workflow": workflow,
                     "p_handle": p_handle,
                     "analysis_module": analysis_module.__name__,
                     "project_id": project.project_id,
                     "run_dir": analysis_dir
                   }
    db = get_shelve_database(config)
    # I don't see how this would ever happen but it makes me nervous to not
    # even check for this.
    #this will happen often.... we need to check that we are not rerunning the same analysis at the same moment
    db_key = "{}_{}_{}_{}".format(project, sample, libprep, fcid)
    if db_key in db:
        error_msg = ("Project {}, Sample {}, Library prep {}, fcid {} "
                     "has an entry in the local db. "
                     "this should not happen --> NEED TO STOP TO AVOID MORE PROBLEMS".format(project, sample, libprep, fcid))
        LOG.warn(error_msg)
        return 

    db[db_key] = project_dict
    db.close()
    LOG.info("Successfully recorded process id {} for Project {}, Sample {}, Library prep {}, fcid {}, "
             "workflow {}".format(p_handle.pid, project, sample, libprep, fcid,   workflow))




def get_shelve_database(config):
    if not config:
        try:
            config_file_path = locate_ngi_config()
            config = load_yaml_config(config_file_path)
        except RuntimeError:
            error_msg = ("No configuration passed and could not find file "
                         "in default locations.")
            raise RuntimeError(error_msg)
    try:
        database_path = config["database"]["record_tracking_db_path"]
    except KeyError as e:
        error_msg = ("Could not get path to process tracking database "
                     "from provided configuration: key missing: {}".format(e))
        raise KeyError(error_msg)
    return shelve.open(database_path)
