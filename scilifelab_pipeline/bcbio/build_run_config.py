#!/bin/env python
"""
Build a bcbio-nextgen run config using info from the SampleSheet.csv and
project info from the StatusDB.
"""
from __future__ import print_function

import argparse
import collections
import couchdb
import datetime
import glob
import functools
import os
import re
import shlex
import shutil
import subprocess
import sys
import yaml

from bcbio.workflow import template
from scilifelab.utils.config import load_yaml_config_expand_vars
from textwrap import dedent

from scilifelab.log import minimal_logger
# Set up logging for this script
LOG = minimal_logger(__name__)

## TODO better error messages for log

## TODO add these to the cl
## Maybe this should just be assumed to have been done via e.g. .bashrc?
PRERUN_ENV_SETUP_CMDS = [ "source activate bcbio-nextgen",
                          "load_modules"]



def build_run_config(samples_dir=None, config_path=None, output_dir=None, upload_dir=None):
    """With a finished directory, generate a configuration file
    and launch bcbio-nextgen via the webserver.

    :param str samples_dir: The path to the directory containing the fastq files.
    :param str config_path: That path to the config file with statusdb auth info,
                            sbatch parameters, and template file locations
    :param str output_dir: Where to store the config & work dirs. Default is a
                           directory named after the sample (or set of samples).
    :param str upload_dir: Where to store the processing output, if different
                           than output_dir.

    :returns: A list of dicts representing runs to execute

    :raises RuntimeError: If unable to connect to StatusDB
    """
    # Load info from config file
    try:
        config_yaml = load_yaml_config_expand_vars(open(config_path, 'r'))
    except (IOError, TypeError) as e:
        LOG.error("Could not open config file for reading; cannot proceed.", file=sys.stderr)
        return None
    try:
        pipeline_mappings = config_yaml["method_to_pipeline_mappings"]
    except KeyError as e:
        LOG.error("Config file must provide library construction method " \
                  "-> pipelines mapping. Cannot proceed.", file=sys.stderr)
        return None
    files = glob.glob("{}/*.fastq*".format(samples_dir))
    supported_genomes = set(config_yaml.get("supported_genomes", []))

    # Connect to database
    status_db_config = config_yaml.get("statusdb")
    try:
        templates = config_yaml["templates"]
    except KeyError as e:
        ## TODO Logging
        print("Config file must provide template file locations. Cannot proceed.", file=sys.stderr)
        raise e
    print("Trying to connect to StatusDB... ", file=sys.stderr)
    couch = couchdb.Server("http://{user}:{password}@{url}:{port}".format(
                    user=status_db_config.get("username"),
                    password=status_db_config.get("password"),
                    url=status_db_config.get("url"),
                    port=status_db_config.get("port")))
    if not couch:
        raise RuntimeError("Couldn't connect to StatusDB or "\
                           "config file lacked authentication information.")

    supported_genomes = set(supported_genomes)
    proj_db = couch['projects']
    file_pairs = find_fastq_read_pairs(input_files)

    samples_to_process = []
    for sample_basename, sample_files in file_pairs.items():
        try:
            project_id = get_project_id_from_filename(sample_basename)
        except ValueError as e:
            # Could not determine project id
            print(e, file=sys.stderr)
            continue
        if not output_dir:
            output_dir = os.path.join(os.path.dirname(os.path.abspath(sample_files[0])), "project_{}".format(sample_basename))
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        if not upload_dir:
            upload_dir = os.path.join(output_dir, "final")
        project_data_json = get_project_data_for_id(project_id, proj_db)
        adapter_seqs      = project_data_json.get("adapter_sequences")
        # Determine the library construction method from the Project
        lib_const_method  = project_data_json.get("details", {}).get("library_construction_method")
        # If we have no matching template, just put it through qc
        pipeline_list     = pipeline_mappings.get(lib_const_method) or ("qc_pipeline_template")
        reference_genome  = project_data_json.get("reference_genome")
        if reference_genome.lower() not in supported_genomes:
            # Unsupported organisms cannot be aligned
            reference_genome = None
            pipeline_list = ("qc_pipeline_template")
        ## TODO At the moment we just have multiple run config files -- each pipeline runs separately.
        ##      I need to think about when this might not work, conflicts and so on.
        ##      May eventually need some way to merge the two template files
        ##      or have template.py modify both pipelines when it adds attributes.
        # This could instead construct and then yield the list of pipelines,
        # if in the future we have pipelines that must run serially
        for template_name in pipeline_list:
            # Get the path to the template file; default is always just qc pipeline
            template_path = templates.get(template_name) or templates.get("qc_pipeline_template")
            namespace_args = argparse.Namespace(template=template_path,
                                                input_files=sample_files,
                                                out_dir=output_dir,
                                                upload_dir=upload_dir)
            if adapter_seqs or reference_genome:
                # Create the csv file that will be passed to the template creator
                project_csv = create_project_csv_from_dbinfo(sample_basename, output_dir,
                                                             adapter_seqs, reference_genome)
                namespace_args.__dict__["metadata"] = project_csv
            ## TODO bcbio.workflow.template doesn't expand $ENV_VARS -- fix
            config_file_path, work_dir = template.setup(namespace_args)
            samples_to_process.append({ 'sample_basename': sample_basename,
                                        'template_name': os.path.basename(template_name),
                                        'run_config': config_file_path,
                                        'work_dir': work_dir})

    ## TODO I might be happier yielding this
    return samples_to_process


## TODO load run params, dist params from configuration file
def build_bcbio_cmd(bcbio_run_params=None, bcbio_dist_params=None):
    """Builds a bcbio_nextgen.py command line to launch processing.

    :param dict bcbio_run_params: Parameters to pass to bcbio-nextgen
    :param dict bcbio_dist_params: Parameters specifying parallelization options

    :returns: A str that can be executed via the shell.
    :rtype: str

    :raises KeyError: if a required bcbio_nextgen parameter is missing
                      (system_config, run_config)
    """
    if not bcbio_run_params:
        bcbio_run_params = {
               "work_dir": work_dir,
               "system_config_path": "$SYSCONFIG",
               "run_config_path": run_config,
               "numcores": 16,}

    # See bcbio_nextgen.py --help for available options
    if not bcbio_dist_params:
        bcbio_dist_params = {"resources": ["account:b2013064","timelimit=4-00:00:00"]}

    # Note that this assumes that the environment executing this commmand will have
    # all the requisite modules loaded and the environment activated
    cl = ["bcbio_nextgen.py"]
    cl.append("-n {} ".format(bcbio_run_params.get("numcores") or "1"))
    if bcbio_run_params.get("work_dir"):
        cl.append("--workdir {} ".format(bcbio_run_params["work_dir"]))
    if bcbio_dist_params:
        cl.append("--scheduler slurm")
        cl.append("--paralleltype ipython")
        cl.append("--queue {}".format(bcbio_dist_params.get("queue") or "core"))
        cl.append("--timeout {}".format(bcbio_dist_params.get("timeout") or "600"))
        cl.append("--retries {}".format(bcbio_dist_params.get("retries") or "1"))
        for res_param in bcbio_dist_params.get("resources") or []:
            cl.append("--resources {}".format(res_param))
    cl.append(bcbio_run_params["system_config_path"])
    cl.append(bcbio_run_params["run_config_path"])
    cl_text = " ".join(cl)
    return cl_text


def find_fastq_read_pairs(file_list):
    """
    Given a list of file names, finds read pairs (based on _R1_/_R2_ file naming)
    and returns a dict of {base_name: [ file_read_one, file_read_two ]}
    E.g.
        1_131129_BH7VPTADXX_P602_101_1.fastq.gz
        1_131129_BH7VPTADXX_P602_101_2.fastq.gz
    becomes
        { "1_131129_BH7VPTADXX_P602_101":
        [ "1_131129_BH7VPTADXX_P602_101_1.fastq.gz",
          "1_131129_BH7VPTADXX_P602_101_2.fastq.gz"]}

    :param list file_list: A list... of files
    :returns: A dict of file_basename -> [files]
    :rtype: collections.defaultdict(list)
    """
    # Remove duplicates
    try:
        file_set = set(file_list)
    except TypeError:
        return {}
    # Split on the read number
    split_pattern = re.compile(r'_\d(\.\w+){0,}.fastq')
    matches_dict = collections.defaultdict(list)
    for file_name in file_list:
        file_basename = os.path.basename(file_name)
        try:
            base = split_pattern.split(file_basename)[0]
            matches_dict[base].append(file_name)
        except IndexError:
            print("Warning: file doesn't match expected file format, "
                  "cannot be paired: \"{}\"".format(file_name), file=sys.stderr)
            matches_dict[file_basename].append(file_name)
    return dict(matches_dict)


def create_project_csv_from_dbinfo(file_basename, output_dir, adapter_seqs, reference_genome):
    """
    Creates a file to be used by the template generator to modify the existing
    template. Output looks like:
        samplename,description,adapters,trim_reads,genome_build
        7_100326_FC6107FAAXX,7_100326_FC6107FAAXX,"[truseq,polya]",read_through,hg19
    Returns the path to the file that is created.
    """
    project_csv_file = os.path.join(output_dir, "{}.csv".format(file_basename))
    if os.path.exists(project_csv_file):
        shutil.move(project_csv_file,
                    project_csv_file + ".bak%s" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    with open(project_csv_file, 'w') as f:
        header_line = ["samplename","description"]
        sample_line = [file_basename,file_basename]
        if adapter_seqs:
            ## TODO Figure out how to deal with adapter sequences -- both fwd and revcom?
            ##      What about mixing default values (e.g. truseq, polya) and custom seqs?
            ##          There is some issue with this in the bcbio code I think
            ##      Maybe it's best just to use custom sequences in case they change upstream
            adapter_seqs = "\"[{}]\"".format(",".join(adapter_seqs))
            header_line.extend(["adapters","trim_reads"])
            sample_line.extend([adapter_seqs,"read_through"])
        if reference_genome:
            header_line.append("genome_build",)
            sample_line.append(reference_genome)
        f.write(",".join(header_line)+"\n")
        f.write(",".join(sample_line)+"\n")
    return project_csv_file


class memoized(object):
    """
    Decorator, caches results of function calls.
    """
    def __init__(self, func):
        self.func   = func
        self.cached = {}
    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if args in self.cached:
            return self.cached[args]
        else:
            return_val = self.func(*args)
            self.cached[args] = return_val
            return return_val
    def __repr__(self):
        return self.func.__doc__
    # This ensures that attribute access (e.g. obj.attr)
    # goes through the __call__ function I defined above
    # functools is awesome
    # descriptors are the raddest
    # boy i love python
    def __get__(self, obj, objtype):
        return functools.partial(self.__call__, obj)


@memoized
def get_project_id_from_filename(sample_basename):
    """Project is pulled from the standard filename format, which is:
       <lane_num>_<date>_<fcid>_<project>_<sample_num>_<read>.fastq[.gz]
    returns the project portion or None if there is no match
    (which shouldn't generally happen).

    :param str sample_basename: The name of the file from which to pull the project id
    :returns: The project id
    :rtype: str
    :raises ValueError: If the project id cannot be determined from the filename
    """
    try:
        project_id = re.match(r'\d_\d{6}_\w{10}_(P\d{3})_.*', sample_basename).groups()[0]
        return project_id
    except (IndexError, AttributeError):
        raise ValueError("Error: filename didn't match conventions, "
                         "couldn't find project id for sample "
                         "\"{}\"".format(sample_basename))


@memoized
def get_project_data_for_id(project_id, proj_db):
    """Pulls all the data about a project from the StatusDB
    given the project's id (e.g. "P602") and a couchdb view object.

    :param str project_id: The project ID
    :param proj_db: The project_db object
    :returns: A dict of the project data
    :rtype: dict
    """
    db_view = proj_db.view('project/project_id')
    try:
        return proj_db.get([proj.id for proj in db_view if proj.key == project_id][0])
    except IndexError:
        # TODO this will be logged and should be caught on the calling side
        raise ValueError("Warning: project ID '{}' not found in Status DB".format(project_id))


if __name__ == "__main__":
    # This entire set of arguments is a stopgap
    parser = argparse.ArgumentParser("Get project information for a sample given "\
                                     "the SampleSheet.csv and a config file "\
                                     "with StatusDB auth info.")
    # TODO this will be a constant I suppose for all runs
    parser.add_argument("-c", "--config", required=True,
            help="The config file containing StatusDB info, template locations, "
                 "and the mapping between library construction methods and pipelines.")
    parser.add_argument("-d", "--input-dir", required=True,
            help="The directory containing the input files.")

    kwargs = vars(parser.parse_args())
    main(kwargs["input_dir"], kwargs["config"])
