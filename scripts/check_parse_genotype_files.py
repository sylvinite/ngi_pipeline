#!/bin/env python

"""Check for the presence of genotype files and update Charon accordingly."""
import argparse
import glob
import os
import re
import time

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.charon import find_projects_from_samples
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.parsers import parse_samples_from_vcf

LOG = minimal_logger(os.path.basename(__file__))


# This may need to be changed
GENOTYPE_FILE_RE = re.compile(r'\d{4}\S*.vcf')

@with_ngi_config
def main(inbox=None, num_days=14, genotype_files=None, config=None, config_file_path=None):
    if genotype_files:
        gt_files_valid = [os.path.abspath(gt_file) for gt_file in genotype_files]
    else:
        if not inbox:
            try:
                inboxes = config["environment"]["flowcell_inbox"]
            except (KeyError, TypeError):
                raise ValueError("No path to delivery inbox specified by argument "
                                 "or in configuration file ({}). Exiting.".format(config_file_path))
        for inbox in inboxes:
            inbox = os.path.abspath(inbox)
            # Convert to seconds
            cutoff_age = time.time() - (int(num_days) * 24 * 60 * 60)
            LOG.info("Searching for genotype files under {} modified after "
                     "{}".format(inbox, time.ctime(cutoff_age)))
            gt_files_valid = []
            for gt_file in filter(GENOTYPE_FILE_RE.match, glob.glob(os.path.join(inbox, "*"))):
                if os.stat(gt_file).st_mtime > time.time() - cutoff_age:
                    gt_files_valid.append(os.path.abspath(gt_file))

    if not gt_files_valid:
        LOG.info("No genotype files found under {} newer than "
                 "{}".format(inbox, time.ctime(cutoff_age)))
    else:
        charon_session = CharonSession()
        for gt_file_path in gt_files_valid:
            project_samples_dict = \
                    find_projects_from_samples(parse_samples_from_vcf(gt_file_path))
            for project_id, samples in project_samples_dict.iteritems():
                LOG.info("Updating project {}...".format(project_id))
                for sample in samples:
                    try:
                        genotype_status = \
                            charon_session.sample_get(projectid=project_id,
                                                      sampleid=sample).get("genotype_status")
                        if genotype_status in (None, "NOT_AVAILABLE"):
                            LOG.info('Updating sample {} genotype_status '
                                     'to "AVAILABLE"...'.format(sample))
                            charon_session.sample_update(projectid=project_id,
                                                         sampleid=sample,
                                                         genotype_status="AVAILABLE")
                        else:
                            LOG.info('Not updating sample {} genotype_status '
                                     '(already "{}")'.format(sample, genotype_status))
                    except CharonError as e:
                        LOG.error('Could not update genotype status to "AVAILABLE" '
                                  'for project/sample "{}/{}": {}'.format(project_id,
                                                                          sample,
                                                                          e))

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Check for genotype data.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", dest="config_file_path",
            help=("Path to the config file to use; if not specified, default "
                  "locations will be checked."))
    parser.add_argument("-d", "--directory", dest="inbox",
            help=("Path to the directory to check for genotyping data. If not "
                  "specified, checks the 'environment.flowcell_inbox' path as "
                  "given in the configuration file."))
    parser.add_argument("-n", "--num-days", type=int, default=14,
            help=("Check for files no older than {num_days} days."))
    parser.add_argument("-g", "--genotype-file", action="append", dest="genotype_files",
            help=("The path to a specific genotype file to parse and use to "
                  "update Charon. If specified, all other args are ignored. "
                  "Use multiple times for multiple files."))

    args = vars(parser.parse_args())

    main(**args)
