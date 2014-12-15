import json
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)


def create_charon_entries_from_project(project, best_practice_analysis="whole_genome_reseq",
                                       sequencing_facility="NGI-S",
                                       force_overwrite=False, delete_existing=False):
    """Given a project object, creates the relevant entries
    in Charon.

    :param NGIProject project: The NGIProject object
    :param str best_practice_analysis: The workflow to assign for this project (default "variant_calling")
    :param str sequencing_facility: The facility that did the sequencing
    :param bool force_overwrite: If this is set to true, overwrite existing entries in Charon (default false)
    :param bool delete_existing: Don't just update existing entries, delete them and create new ones (default false)
    """
    charon_session = CharonSession()
    try:
        status="OPEN"
        LOG.info('Creating project "{}" with status "{}", best practice analysis "{}", '
                 'and sequencing_facility {}'.format(project, status, best_practice_analysis,
                                                   sequencing_facility))
        charon_session.project_create(projectid=project.project_id,
                                      name=project.name,
                                      status=status,
                                      best_practice_analysis=best_practice_analysis,
                                      sequencing_facility=sequencing_facility)
    except CharonError as e:
        if force_overwrite:
            LOG.warn('Overwriting data for project "{}"'.format(project))
            charon_session.project_update(projectid=project.project_id,
                                          name=project.name,
                                          status=status,
                                          best_practice_analysis=best_practice_analysis,
                                          sequencing_facility=sequencing_facility)
        else:
            LOG.info('Project "{}" already exists; moving to samples...'.format(project))

    for sample in project:
        if delete_existing:
            LOG.warn('Deleting existing sample "{}"'.format(sample))
            try:
                charon_session.sample_delete(projectid=project.project_id,
                                             sampleid=sample.name)
            except CharonError as e:
                LOG.error('Could not delete sample "{}": {}'.format(sample, e))
        try:
            analysis_status = "TO_ANALYZE"
            LOG.info('Creating sample "{}" with analysis_status "{}"'.format(sample, analysis_status))
            charon_session.sample_create(projectid=project.project_id,
                                         sampleid=sample.name,
                                         analysis_status=analysis_status)
        except CharonError:
            if force_overwrite:
                LOG.warn('Overwriting data for project "{}" / '
                         'sample "{}"'.format(project, sample))
                charon_session.sample_update(projectid=project.project_id,
                                             sampleid=sample.name,
                                             analysis_status=analysis_status)
            else:
                LOG.info('Project "{}" / sample "{}" already exists; moving '
                         'to libpreps'.format(project, sample))

        for libprep in sample:
            if delete_existing:
                LOG.warn('Deleting existing libprep "{}"'.format(libprep))
                try:
                    charon_session.libprep_delete(projectid=project.project_id,
                                                 sampleid=sample.name,
                                                 libprepid=libprep.name)
                except CharonError as e:
                    LOG.warn('Could not delete libprep "{}": {}'.format(libprep, e))
            try:
                qc= "PASSED"
                LOG.info('Creating libprep "{}" with qc status "{}"'.format(libprep, qc))
                charon_session.libprep_create(projectid=project.project_id,
                                              sampleid=sample.name,
                                              libprepid=libprep.name,
                                              qc=qc)
            except CharonError as e:
                if force_overwrite:
                    LOG.warn('Overwriting data for project "{}" / '
                             'sample "{}" / libprep "{}"'.format(project, sample,
                                                                 libprep))
                    charon_session.libprep_update(projectid=project.project_id,
                                                  sampleid=sample.name,
                                                  libprepid=libprep.name,
                                                  qc=qc)
                else:
                    LOG.info(e)
                    LOG.info('Project "{}" / sample "{}" / libprep "{}" already '
                             'exists; moving to libpreps'.format(project, sample, libprep))

            for seqrun in libprep:
                if delete_existing:
                    LOG.warn('Deleting existing seqrun "{}"'.format(seqrun))
                    try:
                        charon_session.seqrun_delete(projectid=project.project_id,
                                                     sampleid=sample.name,
                                                     libprepid=libprep.name,
                                                     seqrunid=seqrun.name)
                    except CharonError as e:
                        LOG.error('Could not delete seqrun "{}": {}'.format(seqrun, e))
                try:
                    alignment_status="NOT_RUNNING"
                    LOG.info('Creating seqrun "{}" with alignment_status "{}"'.format(seqrun, alignment_status))
                    charon_session.seqrun_create(projectid=project.project_id,
                                                 sampleid=sample.name,
                                                 libprepid=libprep.name,
                                                 seqrunid=seqrun.name,
                                                 alignment_status=alignment_status,
                                                 total_reads=0,
                                                 mean_autosomal_coverage=0)
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
                                                     alignment_status=alignment_status,
                                                     total_reads=0,
                                                     mean_autosomal_coverage=0)
                    else:
                        LOG.info('Project "{}" / sample "{}" / libprep "{}" / '
                                 'seqrun "{}" already exists; next...'.format(project, sample,
                                                                              libprep, seqrun))


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
