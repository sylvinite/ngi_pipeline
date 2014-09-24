import argparse
import time
import random
import string
import os
from os import listdir
from os.path import isfile, join


from ngi_pipeline.conductor.flowcell import process_demultiplexed_flowcell
from ngi_pipeline.conductor.launchers import trigger_sample_level_analysis
from ngi_pipeline.database.process_tracking import check_update_jobs_status
from ngi_pipeline.database.communicate import get_project_id_from_name

from ngi_pipeline.database.classes import CharonSession
import json


def id_generator_chars(size=6, chars=string.ascii_uppercase):
    return ''.join(random.choice(chars) for _ in range(size))

def id_generator_digits(size=6, chars=string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def id_generator_digits_chars(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

data_folder      = "/proj/a2010002/INBOX/"

def main(args):
    originalProject = {}
    originalProject["fc_dir"]           = "/proj/a2010002/INBOX/140702_D00415_0052_AC41A2ANXX/"
    originalProject["fc_name"]          = "140702_D00415_0052_AC41A2ANXX"
    originalProject["fc_id"]            = "C41A2ANXX"
    originalProject["project_name"]     = "M.Kaller_14_06"
    originalProject["project_name_ill"] = "M__Kaller_14_06"
    originalProject["project_id"]       = "P1171"
    originalProject["samples_id"]       = ["102", "104" , "106", "108"]

    ##create random
    rnd_fc_id_noplate  = id_generator_digits_chars(9)
    rnd_fc_id          = "A{}".format(rnd_fc_id_noplate)
    rnd_instrument     = id_generator_digits_chars(6)
    rnd_date           = id_generator_digits(6)
    rnd_fc_name = "{}_{}_{}_{}".format(rnd_date,
                                       rnd_instrument,
                                       id_generator_digits(4),
                                       rnd_fc_id)
    
    rnd_fc_path = os.path.join(data_folder, rnd_fc_name)
    if os.path.isdir(rnd_fc_path):
        print "flowcell name already exists: bad luck!!!! Abort"
        return 1
    rnd_project_name = args.rnd_project_name
    if args.rnd_project_name is "":
        print "error project-name must be specified (something like M.Kaller_14_06)"
        return 1
    
    
    charon_session = CharonSession()


    rndProject = {}
    try:
        rnd_project_id = get_project_id_from_name(rnd_project_name)
        rndProject["project_id"]       = rnd_project_id
        rndProject["project_name"]     = rnd_project_name
    except (RuntimeError, ValueError) as e:
        print " project does not exits on Charon, creating it"
        rnd_project_id   = "P{}".format(id_generator_digits(4))
        rndProject["project_id"]       = rnd_project_id
        rndProject["project_name"]     = rnd_project_name

        base_url = charon_session.construct_charon_url("project")
        project_dict = {'projectid': rndProject["project_id"],
               'name': rndProject["project_name"],
               'status':'SEQUENCED',
               'pipeline':'NGI',
               'best_practice_analysis':'IGN',
               'sequencing_facility':'NGI-S'
               }
        #create the project on charon
        charon_session.post(base_url, json.dumps(project_dict))


    rndProject["fc_dir"]           = rnd_fc_path
    rndProject["fc_name"]          = rnd_fc_name
    rndProject["fc_id"]            = rnd_fc_id

    rndProject["project_name_ill"] = rnd_project_name.replace(".", "__");

    rndProject["samples_id"]       = ["{}".format(id_generator_digits(3)),
                                      "{}".format(id_generator_digits(3)),
                                      "{}".format(id_generator_digits(3)),
                                      "{}".format(id_generator_digits(3))]
    if args.restrict_to_sample is not "":
        originalProject["samples_id"] = [args.restrict_to_sample]
        rndProject["samples_id"]      = ["{}".format(id_generator_digits(3))]




    #TODO: check that this project does not already exists on charon
    os.mkdir(rnd_fc_path)
    #parse SampleSheet_16bp.csv
    parse_sample_sheet("SampleSheet_16bp.csv", originalProject, rndProject)
    #parse SampleSheet.csv
    parse_sample_sheet("SampleSheet.csv", originalProject, rndProject)


    createDir(rndProject["fc_dir"], "Data")
    createDir(rndProject["fc_dir"], "InterOp")
    
    #Unaligned
    createDir(rndProject["fc_dir"], "Unaligned")
    Unaligned_dir           = os.path.join(rndProject["fc_dir"], "Unaligned")
    BaseCall_stats_dir      = "Basecall_Stats_{}".format(rndProject["fc_id"])
    createDir(Unaligned_dir, BaseCall_stats_dir)
    #I do not need to copy the file... I hope as it is madness parse them

    #Unaligned_16bp
    createDir(rndProject["fc_dir"], "Unaligned_16bp")
    Unaligned_path          = os.path.join(rndProject["fc_dir"], "Unaligned_16bp")
    BaseCall_stats_dir      = "Basecall_Stats_{}".format(rndProject["fc_id"])
    createDir(Unaligned_path, BaseCall_stats_dir)
    Project_dir             = "Project_{}".format(rndProject["project_name_ill"])
    createDir(Unaligned_path, Project_dir)
    #need to create samples now
    Project_path = os.path.join( Unaligned_path, Project_dir)
    rndSamplePos = 0;


    for originalSample in originalProject["samples_id"]:
        rndSample     = rndProject["samples_id"][rndSamplePos]
        sample_dir    = "Sample_{}_{}".format(rndProject["project_id"], rndSample)
        createDir(Project_path, sample_dir)
        Sample_path  = os.path.join( Project_path, sample_dir)
        #now hard link or sub-samples fastq files
        originalProject_dir = "Project_{}".format(originalProject["project_name_ill"])
        originalSampleDir   = "Sample_{}_{}".format(originalProject["project_id"], originalSample)
        originalSamplePath  = os.path.join(originalProject["fc_dir"] , "Unaligned_16bp", originalProject_dir, originalSampleDir)
        pairs_to_extract_per_lane  = 0
        
        ##create new sample
        sample_url = charon_session.construct_charon_url("sample", rndProject["project_id"])
        sample_dict = {'sampleid': "{}_{}".format(rndProject["project_id"], rndSample),
               'status':'NEW',
               'received':'2014-04-17',
               'qc_status': 'NEW',
               'genotyping_status': None,
               'genotyping_concordance': None,
               'lims_initial_qc': 'Passed',
               'total_autosomal_coverage': 0,
               'total_sequenced_reads': 0
               }
        charon_session.post(sample_url, json.dumps(sample_dict))
        #create new library prep
        libprep_url = charon_session.construct_charon_url("libprep", rndProject["project_id"], "{}_{}".format(rndProject["project_id"], rndSample))
        libprep_dict = {'libprepid': "A",
               'limsid':'24-44506',
               'status':'NEW'
               }
        charon_session.post(libprep_url, json.dumps(libprep_dict))
        #create seq run


        seqrun_url = charon_session.construct_charon_url("seqrun", rndProject["project_id"], "{}_{}".format(rndProject["project_id"], rndSample), "A")
        seqrun_dict = {'seqrunid': rnd_fc_name  ,
               'sequencing_status':'DONE' ,
#               'mean_autosomal_coverage' : 0
                }
        charon_session.post(seqrun_url, json.dumps(seqrun_dict))




        if args.sample_cov > 0:
            #I know that I have 8 lanes
            reads_to_extract          = (args.sample_cov* 3200000000)/125
            pairs_to_extract          = reads_to_extract/2
            pairs_to_extract_per_lane = pairs_to_extract/8
        
        for fastq in [fastq for fastq in listdir(originalSamplePath) if isfile(join(originalSamplePath,fastq)) and fastq.endswith("fastq.gz")]:
            originalFastq = os.path.join(originalSamplePath, fastq)
            
            rndFastqName  = fastq.replace("{}_{}".format(originalProject["project_id"],  originalSample),
                                          "{}_{}".format(rndProject["project_id"], rndSample))
            rndFastq      = os.path.join(Sample_path , rndFastqName)
            if args.sample_cov == 0:
                os.link(originalFastq, rndFastq)
            else:
                downsample(originalFastq, rndFastq,  pairs_to_extract_per_lane)

        rndSamplePos += 1
                
                
    createDir(Unaligned_dir, "Temp")
    # I try to not consider these guys here
    createDir(Unaligned_dir, "Undetermined_indices")
    # I try to not consider these guys here

    produceRunInfo(rndProject["fc_dir"], rnd_fc_name, rnd_fc_id_noplate, rnd_instrument, rnd_date)
    os.link("/proj/a2010002/INBOX/140702_D00415_0052_AC41A2ANXX/runParameters.xml", os.path.join(rnd_fc_path, "runParameters.xml"))



def produceRunInfo(dir , rnd_fc_name , rnd_fc_id_noplate, rnd_instrument, rnd_date):
    RunInfo_xml  = open(os.path.join(dir, "RunInfo.xml"), "w")
    RunInfo_xml.write("<?xml version=\"1.0\"?>\n")
    RunInfo_xml.write("<RunInfo xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" Version=\"2\">\n")
    RunInfo_xml.write("  <Run Id=\"{}\" Number=\"52\">\n".format(rnd_fc_name))
    RunInfo_xml.write("    <Flowcell>{}</Flowcell>\n".format(rnd_fc_id_noplate))
    RunInfo_xml.write("    <Instrument>{}</Instrument>\n".format(rnd_instrument))
    RunInfo_xml.write("    <Date>{}</Date>\n".format(rnd_date))
    RunInfo_xml.write("    <Reads>\n")
    RunInfo_xml.write("      <Read Number=\"1\" NumCycles=\"126\" IsIndexedRead=\"N\" />\n")
    RunInfo_xml.write("      <Read Number=\"2\" NumCycles=\"8\" IsIndexedRead=\"Y\" />\n")
    RunInfo_xml.write("      <Read Number=\"3\" NumCycles=\"8\" IsIndexedRead=\"Y\" />\n")
    RunInfo_xml.write("      <Read Number=\"4\" NumCycles=\"126\" IsIndexedRead=\"N\" />\n")
    RunInfo_xml.write("    </Reads>\n")
    RunInfo_xml.write("    <FlowcellLayout LaneCount=\"8\" SurfaceCount=\"2\" SwathCount=\"3\" TileCount=\"16\" />\n")
    RunInfo_xml.write("    <AlignToPhiX>\n")
    RunInfo_xml.write("      <Lane>1</Lane>\n")
    RunInfo_xml.write("      <Lane>2</Lane>\n")
    RunInfo_xml.write("      <Lane>3</Lane>\n")
    RunInfo_xml.write("      <Lane>4</Lane>\n")
    RunInfo_xml.write("      <Lane>5</Lane>\n")
    RunInfo_xml.write("      <Lane>6</Lane>\n")
    RunInfo_xml.write("      <Lane>7</Lane>\n")
    RunInfo_xml.write("      <Lane>8</Lane>\n")
    RunInfo_xml.write("    </AlignToPhiX>\n")
    RunInfo_xml.write("  </Run>\n")
    RunInfo_xml.write("</RunInfo>\n")



    


def downsample(orginalFastq, downsampledFastq, pairs_to_extract):
    lines_to_extract = pairs_to_extract*4
    print "downsampling {}".format(orginalFastq)
    #print "zcat {} | head -n {} | gzip   > {} ".format(orginalFastq, lines_to_extract, downsampledFastq)
    from signal import signal, SIGPIPE, SIG_DFL
    signal(SIGPIPE,SIG_DFL)
    os.system("zcat {} | head -n {} | gzip  > {} ".format(orginalFastq, lines_to_extract, downsampledFastq))
    return

def createDir(rootDir, Dir):
    os.mkdir(os.path.join(rootDir, Dir))
    return

def parse_sample_sheet(sample_sheet, originalProject, rndProject):
    rnd_SampleSheet  = open(os.path.join(rndProject["fc_dir"], sample_sheet), "w")
    i = -1;
    for line in open(os.path.join(originalProject["fc_dir"], sample_sheet),'r'):
        if i == -1:
            i=0 #header line
        else:
            line = line.replace(originalProject["project_name_ill"], rndProject["project_name_ill"])
            line = line.replace(originalProject["fc_id"], rndProject["fc_id"])
            line = line.replace(originalProject["project_id"], rndProject["project_id"])
            if len(rndProject["samples_id"]) > 1:
                line = line.replace(originalProject["samples_id"][i], rndProject["samples_id"][i])
                i+=1
                if i == 4:
                    i = 0
            elif line.find("{}_{}".format(rndProject["project_id"], rndProject["samples_id"][0] )) > -1:
                line = line.replace(originalProject["samples_id"][0], rndProject["samples_id"][0])

        rnd_SampleSheet.write(line)

    rnd_SampleSheet.close()
    return




if __name__ == '__main__':
    parser = argparse.ArgumentParser("Creates for simulation purpose a new fc and populates charon accordingly.")
    parser.add_argument("--rnd-project-name", default="",  action="store", help="name of the fake project (something like M.Kaller_14_06)")
    parser.add_argument("--restrict-to-sample", default="", action="store", help="create a fc with only the specified sample (one between 102, 104, 106, 108)" )
    parser.add_argument("--sample-cov", action="store",   default=0, type=int, help="limit the raw coverage to what specified here" )
    
    args = parser.parse_args()
    main(args)


