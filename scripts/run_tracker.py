#!/usr/bin/env python
import argparse
import csv
import glob
import os

from ngi_pipeline.log import loggers
from ngi_pipeline.utils import config as cf

DESCRIPTION =(" Script to keep track and pre-process Illumina X Ten runs. "

"The script will work only with X Ten runs. X Ten runs generate different file "
"structure and naming than HiSeq 2000/2500. To run this script you will also need "
"bcl2fastq V >= 2.1."

"Once a run is completed and it has been pre-processed, demultiplexed data will be "
"sent to the processing server/HPC indicated in the configuration file.")

LOG = loggers.minimal_logger('Run tracker')

def check_config_options(config):
    """ Check that all needed configuration sections/config are present

    :param dict config: Parsed configuration file
    """
    try:
        config['preprocessing']
        config['preprocessing']['hiseq_data']
        config['preprocessing']['miseq_data']
        config['preprocessing']['mfs']
        config['preprocessing']['bcl2fastq']
        config['preprocessing']['remote']
        config['preprocessing']['remote']['user']
        config['preprocessing']['remote']['host']
        config['preprocessing']['remote']['data_archive']
    except KeyError:
        raise RuntimeError(("Required configuration config not found, please "
            "refer to the README file."))


def is_finished(run):
    """ Checks if a run is finished or not. Check corresponding status file

    :param str run: Run directory
    """
    return os.path.exists(os.path.join(run, 'RTAComplete.txt'))


def processing_status(run):
    """ Returns the processing status of a sequencing run. Status are:

        TO_START - The BCL conversion and demultiplexing process has not yet started 
        IN_PROGRESS - The BCL conversion and demultiplexing process is started but not completed
        COMPLETED - The BCL conversion and demultiplexing process is completed

    :param str run: Run directory
    """
    demux_dir = os.path.join(run, 'Demultiplexing')
    if not os.path.exists(demux_dir):
        return 'TO_START'
    elif os.path.exists(os.path.join(demux_dir, 'Stats', 'DemultiplexingStats.xml')):
        return 'COMPLETED'
    else:
        return 'IN_PROGRESS'


def is_transfered(run, transfer_file):
    """ Checks wether a run has been transferred to the analysis server or not

    :param str run: Run directory
    :param str transfer_file: Path to file with information about transfered runs
    """
    try:
        with open(transfer_file, 'r') as f:
            t_f = csv.reader(f, delimiter='\t')
            for row in t_f:
                #Rows have two columns: run and transfer date
                if row[0] == run:
                    return True
            return False
    except IOError:
        return False


def transfer_run(run, config):
    """ Transfer a run to the analysis server. Will add group R/W permissions to
    the run directory in the destination server so that the run can be processed
    by any user/account in that group (i.e a functional account...)

    :param str run: Run directory
    :param dict config: Parsed configuration
    """
    cl = ['rsync', '-a', '--chmod=g+rw']
    r_user = config['remote']['user']
    r_host = config['remote']['host']
    r_dir = config['remote']['data_archive']
    remote = "{}@{}:{}".format(r_user, r_host, r_dir)
    cl.extend([remote, run])


if __name__=="__main__":
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--config', type=str, help='Config file for the NGI pipeline')
    args = parser.parse_args()
    
    if not args.config:
        # Will raise RuntimeError if not config file is found
        args.config = cf.locate_ngi_config()

    config = cf.load_yaml_config(args.config)
    check_config_options(config)
    config = config['preprocessing']

    hiseq_runs = glob.glob(os.path.join(config['hiseq_data'], '1*XX'))
    for run in hiseq_runs:
        run_name = os.path.basename(run)
        LOG.info('Checking run {}'.format(run_name))
        if is_finished(run):
            status = processing_status(run)
            if  status == 'TO_START':
                LOG.info(("Starting BCL to FASTQ conversion and demultiplexing for "
                    "run {}".format(run_name)))
            elif status == 'IN_PROGRESS':
                LOG.info(("BCL conversion and demultiplexing process in progress for "
                    "run {}, skipping it".format(run_name)))
            elif status == 'COMPLETED':
                LOG.info(("Processing of run {} if finished, check if run has been "
                    "transfered and transfer it otherwise".format(run_name)))
                transferred = is_transfered(run_name, config['transfer_file'])
                if not transferred:
                    LOG.info("Run {} hasn't been transfered yet.".format(run_name))
                    LOG.info('Transferring run {} to {} into {}'.format(run_name,
                        config['remote']['host'],
                        config['remote']['data_archive']))
                    transfer_run(run, config)
                else:
                    LOG.info('Run {} already transferred to analysis server, skipping it'.format(run_name))

        if not is_finished(run):
            # Check status files and say i.e Run in second read, maybe something
            # even more specific like cycle or something
            LOG.info('Run {} is not finished yet'.format(run_name))