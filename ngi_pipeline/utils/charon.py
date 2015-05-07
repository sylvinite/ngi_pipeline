import collections
import re

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)

def find_projects_from_samples(sample_list):
    """Given a list of samples, attempts to determine
    which projects they belong to using Charon records.

    :param list sample_list: A list of the samples for which to find projects

    :returns: a dict of {project_id: set(samples)}
    :rtype: dict of sets

    :raises ValueError: If you fail to pass in a list. Nice work!
    """
    STHLM_SAMPLE_RE = re.compile(r'(P\d{4})_')
    projects_dict = collections.defaultdict(set)
    samples_by_project_id = {}
    no_owners_found = set()
    multiple_owners_found = set()
    charon_session = CharonSession()
    if not type(sample_list) is list:
        raise ValueError("Input should be list.")

    for sample_name in sample_list:
        # First see if we can just parse out the project id from the sample name
        m = STHLM_SAMPLE_RE.match(sample_name)
        if m:
            project_id = m.groups()[0]
            try:
                # Ensure that we guessed right
                charon_session.project_get_sample(project_id, sample_name)
            except CharonError as e:
                LOG.debug('Project for sample "{}" appears to be "{}" but is not '
                          'present in Charon ({})'.format(sample_name, project_id, e))
                no_owners_found.add(sample_name)
            else:
                projects_dict[project_id].add(sample_name)
        else:
            # Otherwise check all the projects for matching samples (returns list or None)
            owner_projects_list = charon_session.sample_get_projects(sample_name)
            if not owner_projects_list:
                no_owners_found.add(sample_name)
            elif len(owner_projects_list) > 1:
                multiple_owners_found.add(sample_name)
            else:
                projects_dict[owner_projects_list[0]].add(sample_name)
    if no_owners_found:
        LOG.warn("No projects found for the following samples: {}".format(", ".join(no_owners_found)))
    if multiple_owners_found:
        LOG.warn('Multiple projects found with the following samples (owner '
                 'could not be unamibugously determined): {}'.format(", ".join(multiple_owners_found)))
    return dict(projects_dict)
