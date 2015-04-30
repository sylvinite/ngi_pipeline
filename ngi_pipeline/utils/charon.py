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
    """
    STHLM_SAMPLE_RE = re.compile(r'(P\d{4})_')
    projects_dict = collections.defaultdict(set)
    samples_by_project_id = {}
    no_owners_found = set()
    all_projects = None
    charon_session = CharonSession()

    for sample_name in sample_list:
        # First see if we can just parse out the project id from the sample name
        m = STHLM_SAMPLE_RE.match(sample_name)
        if m:
            project_id = m.groups()[0]
            projects_dict[project_id].add(sample_name)
        else:
        # Alright then we'll do this the hard way. I'm looking at you, Uppsala
        # This is probably better implemented within Charon. I'm looking at you, Denis
        # Also this will just grab the first project containing the sample --
        # if it exists in more than one project I don't know what to tell you
        # except that your sample naming scheme sucks.
            if not all_projects:
                all_projects = [project["projectid"] for project in \
                                charon_session.projects_get_all()['projects']]
            for project_id in all_projects:
                owner_project_id = None
                if not samples_by_project_id.get(project_id):
                    try:
                        samples = charon_session.project_get_samples(projectid=project_id)['samples']
                    except CharonError as e:
                        pass
                    samples_by_project_id[project_id] = [sample['sampleid'] for sample in samples]
                if sample_name in samples_by_project_id[project_id]:
                    owner_project_id = project_id
                    break
            if owner_project_id:
                projects_dict[str(owner_project_id)].add(sample_name)
            else:
                no_owners_found.add(sample_name)
    if no_owners_found:
        LOG.warn("No projects found for the following samples: {}".format(", ".join(no_owners_found)))
    return dict(projects_dict)
