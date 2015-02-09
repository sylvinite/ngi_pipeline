from __future__ import print_function

import argparse
import sys

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.communicate import get_project_id_from_name

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project", required=True)
    parser.add_argument("-s", "--sample", required=True)
    parser.add_argument("-c", "--coverage", type=int, required=True, dest="required_coverage")

    args = parser.parse_args()

    project = args.project
    sample = args.sample
    required_coverage = args.required_coverage

    charon_session = CharonSession()
    try:
        reported_coverage = charon_session.sample_get(project, sample).get("total_autosomal_coverage")
    except CharonError as e:
        try:
            project = get_project_id_from_name(project)
        except (CharonError, RuntimeError, ValueError) as e:
            print(('ERROR: Could not determine coverage for project {} / sample '
                    '{}: {}'.format(project, sample, e)), file=sys.stderr)
            reported_coverage = 0
        else:
            reported_coverage = charon_session.sample_get(project, sample).get("total_autosomal_coverage")
    if int(reported_coverage) >= int(required_coverage):
        sys.exit(0)
    else:
        sys.exit(1)
