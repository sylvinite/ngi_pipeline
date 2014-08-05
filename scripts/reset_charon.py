"""Keeps track of running workflow processes"""
import json
import shelve
import os
import glob
import re

from ngi_pipeline.database import construct_charon_url, get_charon_session
from ngi_pipeline.log import minimal_logger
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from ngi_pipeline.database import get_project_id_from_name
from ngi_pipeline.utils.parsers import parse_genome_results


if __name__ == '__main__':
    
    
    charon_session = get_charon_session()
    url = construct_charon_url("projects")
    
    projects_response = charon_session.get(url).json()
    
    projects_to_clean = ("P1142", "P1171", "P567")
    
    for project in projects_response["projects"]:
        if project["projectid"] in projects_to_clean:
            project["pipeline"] = "NGI"
            project["best_practice_analysis"] = "IGN"
            url = construct_charon_url("project", project["projectid"])
            charon_session.put(url, json.dumps(project))
            
            """
            url = construct_charon_url("samples", project["projectid"])
            samples_response = charon_session.get(url).json()
            for sample in samples_response["samples"]:
                if "total_autosomal_coverage" in sample:
                    del sample["total_autosomal_coverage"]
                sample["total_autosomal_coverage"] = 0.0
                if "status" in sample:
                    del sample["status"]
                url = construct_charon_url("sample", project["projectid"], sample["sampleid"])
                charon_session.put(url, json.dumps(sample))
            
            url =construct_charon_url("seqruns", project["projectid"])
            seqruns_response = charon_session.get(url).json()
            for seqrun in seqruns_response["seqruns"]:
                fields_to_delete = (
                        'mean_autosome_coverage',
                        'mean_coverage',
                        'std_coverage',
                        'aligned_bases',
                        'mapped_bases',
                        'mapped_reads',
                        'reads',
                        'sequenced_bases',
                        'bam_file',
                        'output_file',
                        'GC_percentage',
                        'mean_mapping_quality',
                        'bases_number',
                        'contigs_number',
                        'lanes'
                        'alignment_status'
                        )
                
                for field in fields_to_delete:
                    if field in seqrun:
                        del(seqrun[field])
                
                #import pdb
                #pdb.set_trace()
                
                seqrun['mean_autosome_coverage'] = 0

                url = construct_charon_url("seqrun", seqrun["projectid"],
                    seqrun["sampleid"], seqrun["libprepid"], seqrun["seqrunid"])
                charon_session.put(url, json.dumps(seqrun))


            """




