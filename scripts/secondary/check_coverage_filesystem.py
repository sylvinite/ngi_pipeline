from __future__ import print_function

import argparse
import sys

from ngi_pipeline.engines.piper_ngi.local_process_tracking import _parse_mean_coverage_from_qualimap

if __name__=="__main__":
    parser = argparse.ArgumentParser(("Determine if a particular sample or sequencing run "
                                      "has been sequenced to sufficient coverage using "
                                      "the output from qualimap produced by Piper. "
                                      "Return code indicates yes/no (0/1)."))
    parser.add_argument("-p", "--qc-path", required=True,
            help=("The path to the Piper qualimap qc directory"))
    parser.add_argument("-s", "--sample", required=True,
            help=("For instance 'P1170_105'"))
    g = parser.add_mutually_exclusive_group()
    g.add_argument("-r", "--run",
            help=("For instance '140821_D00458_0029_AC45JGANXX'; specify either this or fcid"))
    g.add_argument("-f", "--fcid",
            help=("For instance 'AC45JGANXX'; specify either this or run"))
    parser.add_argument("-c", "--coverage", type=int, dest="required_coverage")

    args = parser.parse_args()

    qc_path = args.qc_path
    sample = args.sample
    run = args.run if args.run else None
    fcid = args.fcid if args.fcid else None
    required_coverage = args.required_coverage

    reported_coverage = _parse_mean_coverage_from_qualimap(qc_path, sample, run, fcid)

    print("Coverage is {}".format(reported_coverage))
    if required_coverage:
        if int(reported_coverage) >= int(required_coverage):
            sys.exit(0)
        else:
            sys.exit(1)
