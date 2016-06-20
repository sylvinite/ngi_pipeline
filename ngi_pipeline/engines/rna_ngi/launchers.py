
import os
import glob

from ngi_pipeline.engines.rna_ngi.local_process_tracking import record_project_job
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.communication import mail_analysis
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.engines.utils import handle_sample_status, handle_libprep_status, handle_seqrun_status
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.utils.filesystem import load_modules, execute_command_line, \
                                          safe_makedir, do_symlink
                                          


LOG = minimal_logger(__name__)

@with_ngi_config
def analyze(analysis_object, config=None, config_file_path=None):

    charon_session = CharonSession()
    charon_pj=charon_session.project_get(analysis_object.project.project_id)
    reference_genome=charon_pj.get('reference')
    fastq_files=[]
    if reference_genome and reference_genome != 'other':
        for sample in analysis_object.project:
            try:
                charon_reported_status = charon_session.sample_get(analysis_object.project.project_id,
                                                                   sample).get('analysis_status')
                # Check Charon to ensure this hasn't already been processed
                do_analyze=handle_sample_status(analysis_object, sample, charon_reported_status)
                if not do_analyze :
                    continue
            except CharonError as e:
                LOG.error(e)

            for libprep in sample:
                charon_lp_status=charon_session.libprep_get(analysis_object.project.project_id, sample.name, libprep.name).get('qc')
                do_analyze=handle_libprep_status(analysis_object, libprep, charon_lp_status)
                if not do_analyze :
                    continue
                else:
                    for seqrun in libprep:
                        charon_sr_status=charon_session.seqrun_get(analysis_object.project.project_id, sample.name, libprep.name, seqrun.name).get('alignment_status')
                        do_analyze=handle_seqrun_status(analysis_object, seqrun, charon_sr_status)
                        if not do_analyze :
                            continue
                        else:
                            seqrun.being_analyzed=True
                            sample.being_analyzed = sample.being_analyzed or True
                            for fastq_file in seqrun.fastq_files:
                                fastq_path=os.path.join(analysis_object.project.base_path, "DATA", analysis_object.project.project_id, sample.name, libprep.name, seqrun.name, fastq_file)
                                fastq_files.append(fastq_path)
        
        if not fastq_files:
            LOG.error("No fastq files obtained for the analysis fo project {}, please check the Charon status.".format(analysis_object.project.name))
        else :
            fastq_dir=preprocess_analysis(analysis_object, fastq_files)
            sbatch_path=write_batch_job(analysis_object, reference_genome, fastq_dir)
            job_id=start_analysis(sbatch_path)
            analysis_path=os.path.join(analysis_object.project.base_path, "ANALYSIS", analysis_object.project.project_id, 'rna_ngi')
            record_project_job(analysis_object.project, job_id, analysis_path)
        

def start_analysis(sbatch_path):
    cl=["bash", sbatch_path]
    handle=execute_command_line(cl)
    return handle.pid



def preprocess_analysis(analysis_object, fastq_files):
    analysis_path=os.path.join(analysis_object.project.base_path, "ANALYSIS", analysis_object.project.project_id, 'rna_ngi')
    safe_makedir(analysis_path)
    convenience_dir_path=os.path.join(analysis_path, 'fastqs')
    safe_makedir(convenience_dir_path)
    LOG.info("cleaning subfolder {}".format(convenience_dir_path))
    for link in glob.glob(os.path.join(convenience_dir_path, '*')):
        os.unlink(link)
    do_symlink(fastq_files, convenience_dir_path)
    return convenience_dir_path


@with_ngi_config
def write_batch_job(analysis_object, reference, fastq_dir_path, config=None, config_file_path=None):
    analysis_path=os.path.join(analysis_object.project.base_path, "ANALYSIS", analysis_object.project.project_id, 'rna_ngi')
    sbatch_dir_path=os.path.join(analysis_path, 'sbatch')
    safe_makedir(sbatch_dir_path)
    sbatch_file_path=os.path.join(sbatch_dir_path, 'rna_ngi.sh')
    fastq_glob_path=os.path.join(fastq_dir_path, '*_R{1,2}_*.fastq.gz')
    main_nexflow_path=config['analysis']['best_practice_analysis']['RNA-seq']['ngi_nf_path']
    nf_conf=config['analysis']['best_practice_analysis']['RNA-seq']['ngi_conf']
    analysis_log_path=os.path.join(analysis_path, 'nextflow_output.log')
    exit_code_path=os.path.join(analysis_path, 'nextflow_exit_code.out')
    LOG.info("Writing sbatch file to {}".format(sbatch_file_path))
    with open(sbatch_file_path, 'w') as sb:
        sb.write("#!/bin/bash\n\n")
        sb.write("cd {an_path}\n".format(an_path=analysis_path))
        sb.write("> {ex_path}\n".format(ex_path=exit_code_path))
        sb.write("nextflow {ngi_rna_nf} --reads '{fastq_glob}' --genome '{ref}' -c {nf_conf} --outdir {an_path} &> {out_log}\n".format(
            ngi_rna_nf=main_nexflow_path,fastq_glob=fastq_glob_path, ref=reference, nf_conf=nf_conf, an_path=analysis_path, out_log=analysis_log_path))
        sb.write("echo $? > {ex_path}\n".format(ex_path=exit_code_path))
    LOG.info("NextFlow output will be logged at {}".format(analysis_log_path))
    return sbatch_file_path



        
