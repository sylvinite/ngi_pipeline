#!/bin/env python
"""Print the analysis status for samples, libpreps, and seqruns within a project."""

from __future__ import print_function

import argparse
import functools
import os
import pprint
import subprocess
import sys
import tempfile
import time

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.engines.piper_ngi.local_process_tracking import update_charon_with_local_jobs_status
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import locate_flowcell, locate_project

print_stderr = functools.partial(print, file=sys.stderr)

def project_summarize(projects, verbose=False):
    update_charon_with_local_jobs_status(quiet=True) # Don't send mails
    charon_session = CharonSession()
    projects_list = []
    for project in projects:
        print_stderr('Gathering information for project "{}"...'.format(project))
        project_dict = {}
        try:
            # Locate project in Charon and print the value of all its various members.
            project = charon_session.project_get(project)
        except CharonError as e:
            print_stderr('Project "{}" not found in Charon; skipping ({})'.format(project, e), file=sys.stderr)
            continue
        project_dict['name'] = project['name']
        project_dict['id'] = project['projectid']
        project_dict['status'] = project['status']
        samples_list = project_dict['samples'] = []
        for sample in charon_session.project_get_samples(project['projectid']).get('samples', []):
            sample_dict = {}
            sample_dict['id'] = sample['sampleid']
            sample_dict['analysis_status'] = sample['analysis_status']
            sample_dict['coverage'] = sample['total_autosomal_coverage']
            libpreps_list = sample_dict['libpreps'] = []
            samples_list.append(sample_dict)
            for libprep in charon_session.sample_get_libpreps(project['projectid'],
                                                              sample['sampleid']).get('libpreps', []):
                libprep_dict = {}
                libprep_dict['id'] = libprep['libprepid']
                libprep_dict['qc'] = libprep['qc']
                seqruns_list = libprep_dict['seqruns'] = []
                libpreps_list.append(libprep_dict)
                for seqrun in charon_session.libprep_get_seqruns(project['projectid'],
                                                                 sample['sampleid'],
                                                                 libprep['libprepid']).get('seqruns', []):
                    seqrun_dict = {}
                    seqrun_dict['id'] = seqrun['seqrunid']
                    seqrun_dict['alignment_status'] = seqrun['alignment_status']
                    seqrun_dict['coverage'] = seqrun['mean_autosomal_coverage']
                    if seqrun.get('total_reads'):
                        seqrun_dict['total_reads'] = seqrun['total_reads']
                    seqruns_list.append(seqrun_dict)
        projects_list.append(project_dict)

    if verbose:
        output_template = "{}{:<30}{:>{rspace}}"
        for project_dict in projects_list:
            offset = 0
            indent = " " * offset
            rspace = 80 - offset
            print_stderr(output_template.format(indent, "Project name:", project['name'], rspace=rspace))
            print_stderr(output_template.format(indent, "Project ID:", project['id'], rspace=rspace))
            print_stderr(output_template.format(indent, "Project status:", project['status'], rspace=rspace))
            for sample_dict in project_dict['samples']:
                print_stderr("")
                offset = 4
                indent = " " * offset
                rspace = 80 - offset
                print_stderr(output_template.format(indent, "Sample ID:", sample_dict['id'], rspace=rspace))
                print_stderr(output_template.format(indent, "Sample analysis status:", sample_dict['analysis_status'], rspace=rspace))
                print_stderr(output_template.format(indent, "Sample coverage:", sample_dict['coverage'], rspace=rspace))
                for libprep_dict in sample_dict['libpreps']:
                    print_stderr("")
                    offset = 8
                    indent = " " * offset
                    rspace = 80 - offset
                    print_stderr(output_template.format(indent, "Libprep ID:", libprep_dict['id'], rspace=rspace))
                    print_stderr(output_template.format(indent, "Libprep qc status:", libprep_dict['qc'], rspace=rspace))
                    for seqrun_dict in libprep_dict['seqruns']:
                        print_stderr("")
                        offset = 12
                        indent = " " * offset
                        rspace = 80 - offset
                        print_stderr(output_template.format(indent, "Seqrun ID:", seqrun_dict['id'], rspace=rspace))
                        print_stderr(output_template.format(indent, "Seqrun alignment status:", seqrun_dict['alignment_status'], rspace=rspace))
                        print_stderr(output_template.format(indent, "Seqrun mean auto. coverage:", seqrun_dict['coverage'], rspace=rspace))
                        if "total_reads" in seqrun_dict:
                            print_stderr(output_template.format(indent, "Seqrun total reads:", seqrun_dict['total_reads'], rspace=rspace))
            print_stderr("\n")


def flowcell_summarize(flowcells):
    for flowcell in flowcells:
        try:
            flowcell_dir = locate_flowcell(flowcell)
        except ValueError as e:
            print_stderr('Could not locate flowcell "{}"; skipping ({})'.format(flowcell, e), file=sys.stderr)
        # Get the projects from the flowcell dir and pass them to project_summarize


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true",
            help="Print extended info format")
    subparsers = parser.add_subparsers(help="Summarize project or flowcell.")
    project_parser = subparsers.add_parser('project')
    project_parser.add_argument('project_dirs', nargs='+',
            help=('The name or ID (in Charon) of one or more projects to be summarized.'))

    flowcell_parser = subparsers.add_parser('flowcell')
    flowcell_parser.add_argument('flowcell_dirs', nargs='+',
            help=('The path to or name of one or more flowcell directories to be summarized.'))


    args = parser.parse_args()

    if "project_dirs" in args:
        project_summarize(args.project_dirs, args.verbose)
    elif "flowcell_dirs" in args:
        flowcell_summarize(args.flowcell_dirs, args.verbose)
    else:
        parser.print_usage()
