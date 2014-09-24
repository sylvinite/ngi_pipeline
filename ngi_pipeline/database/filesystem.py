import json
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)


## FIXME this is a hack, improve it soon/later/someday
##       (/never):q
def create_charon_entries_from_project(project, workflow="NGI", force_overwrite=False):
    """Given a project object, creates the relevant entries
    in Charon.

    :param NGIProject project: The NGIProject object
    :param str workflow: The workflow to assign for this project (default NGI)
    :param bool force_overwrite: If this is set to true, overwrite existing entries in Charon (default false)
    """
    charon_session = CharonSession()
    try:
        status="SEQUENCED"
        LOG.info('Creating project "{}" with status "{}" and workflow "{}"'.format(project, status, workflow))
        charon_session.project_create(projectid=project.project_id,
                                      name=project.name,
                                      status=status,
                                      pipeline=workflow)
    except CharonError:
        if force_overwrite:
            LOG.warn('Deleting project "{}" from Charon'.format(project))
            LOG.info('Creating project "{}" with status "{}" and workflow "{}"'.format(project, status, workflow))
            charon_session.project_delete(projectid=project.project_id)
            charon_session.project_create(projectid=project.project_id,
                                          name=project.name,
                                          status=status,
                                          pipeline=workflow)
        else:
            raise
    for sample in project:
        try:
            LOG.info('Creating sample "{}"'.format(sample))
            charon_session.sample_create(projectid=project.project_id,
                                         sampleid=sample.name,
                                         status="NEW")
        except CharonError:
            if force_overwrite:
                LOG.warn('Overwriting data for project "{}" / '
                         'sample "{}"'.format(project, sample))
                charon_session.sample_update(projectid=project.project_id,
                                             sampleid=sample.name,
                                             status="NEW")
            else:
                raise
        for libprep in sample:
            try:
                LOG.info('Creating libprep "{}"'.format(libprep))
                charon_session.libprep_create(projectid=project.project_id,
                                              sampleid=sample.name,
                                              libprepid=libprep.name,
                                              status="NEW")
            except CharonError:
                if force_overwrite:
                    LOG.warn('Overwriting data for project "{}" / '
                             'sample "{}" / libprep "{}"'.format(project, sample,
                                                                 libprep))
                    charon_session.libprep_update(projectid=project.project_id,
                                                  sampleid=sample.name,
                                                  libprepid=libprep.name,
                                                  status="NEW")
                else:
                    raise
            for seqrun in libprep:
                try:
                    LOG.info('Creating seqrun "{}"'.format(seqrun))
                    charon_session.seqrun_create(projectid=project.project_id,
                                                 sampleid=sample.name,
                                                 libprepid=libprep.name,
                                                 seqrunid=seqrun.name,
                                                 total_reads=0,
                                                 mean_autosomal_coverage=0,
                                                 sequencing_status="DONE",
                                                 alignment_status="NEW")
                except CharonError as e:
                    if force_overwrite:
                        LOG.warn('Overwriting data for project "{}" / '
                                 'sample "{}" / libprep "{}" / '
                                 'seqrun "{}"'.format(project, sample,
                                                      libprep, seqrun))
                        charon_session.seqrun_update(projectid=project.project_id,
                                                     sampleid=sample.name,
                                                     libprepid=libprep.name,
                                                     seqrunid=seqrun.name,
                                                     status="NEW")
                    else:
                        raise


def recreate_project_from_db(analysis_top_dir, project_name, project_id):
    project_dir = os.path.join(analysis_top_dir, "DATA", project_name)
    project_obj = NGIProject(name=project_name,
                             dirname=project_name,
                             project_id=project_id,
                             base_path=analysis_top_dir)
    charon_session = CharonSession()
    try:
        samples_dict = charon_session.project_get_samples(project_id)["samples"]
    except CharonError as e:
        raise RuntimeError("Could not access samples for project {}: {}".format(project_id, e))
    for sample in samples_dict:
        sample_id = sample.get("sampleid")
        sample_dir = os.path.join(project_dir, sample_id)
        sample_obj = project_obj.add_sample(name=sample_id, dirname=sample_id)
        sample_obj.status = sample.get("status", "unknown")
        try:
            libpreps_dict = charon_session.sample_get_libpreps(project_id, sample_id)["libpreps"]
        except CharonError as e:
            raise RuntimeError("Could not access libpreps for project {} / sample {}: {}".format(project_id,sample_id, e))
        for libprep in libpreps_dict:
            libprep_id = libprep.get("libprepid")
            libprep_obj = sample_obj.add_libprep(name=libprep_id,  dirname=libprep_id)
            libprep_obj.status = libprep.get("status", "unknown")
            try:
                seqruns_dict = charon_session.libprep_get_seqruns(project_id, sample_id, libprep_id)["seqruns"]
            except CharonError as e:
                raise RuntimeError("Could not access seqruns for project {} / sample {} / "
                                   "libprep {}: {}".format(project_id, sample_id, libprep_id, e))
            for seqrun in seqruns_dict:
                # e.g. 140528_D00415_0049_BC423WACXX
                seqrun_id = seqrun.get("seqrunid")
                seqrun_obj = libprep_obj.add_seqrun(name=seqrun_id, dirname=seqrun_id)
                seqrun_obj.status = seqrun.get("status", "unknown")
    return project_obj
