import os
import glob
import re
import sys
import argparse
import random
import requests
import json
import datetime
import shutil


def main(args):
    if args.delete is not None:
        print "deleting project {} from charon".format(args.delete)
        cleanCharon(args.delete,args.url, args.token)
        print "I am leazy and I am not going to do nothing more than this"
        return 0

    # create fake project name and fake project idenitifier
    (project_name, project_identifier) = create_proj_name(args.project_name, args.facility)
    # create the data structure that will contains all the project, samples, libraries, runs
    charon_data = {}
    charon_data['projectid'] = project_identifier
    charon_data['name'] = project_name
    charon_data['pipeline']="TEST"
    charon_data['sequencing_facility'] = "NGI-S" if args.facility == 'stockholm' else "NGI-U"
    charon_data['best_practice_analysis']="whole_genome_reseq"
    charon_data['status']='OPEN'
    charon_data['pipeline']='NGI'
    charon_data['samples']={}
    # now iterate over all FC I need to create
    sample_number = 1 # this needs to keep count of the samples
    for i in xrange(args.FC):
        #create fake FC name
        FC_name = create_fake_FC()
        # create directory
        os.mkdir(FC_name)
        # create SampleSheet.csv
        touch(os.path.join(FC_name, "SampleSheet.csv"))
        # create folder Demultiplexing
        os.mkdir(os.path.join(FC_name, "Demultiplexing"))
        # create folder Demultiplexing/Reports
        os.mkdir(os.path.join(FC_name, "Demultiplexing", "Reports"))
        # create folder Demultiplexing/Stats
        os.mkdir(os.path.join(FC_name, "Demultiplexing", "Stats"))
        # create folder Demultiplexing/projet_name (replace "." with "_")
        project_name_underscore = project_name.replace(".", "_") # this should work with Uppsala also
        os.mkdir(os.path.join(FC_name, "Demultiplexing", project_name_underscore))
        # for each lane create a sample (make stockholm uppsala difference)
        print "producing FC: {}".format(FC_name)
        for lane in (1,2,3,4,5,6,7,8):
            # create fake sample names, start easy, one sample per lane
            sample_name = create_sample_name(project_identifier, sample_number, args.facility)
            # create the sample folder
            sample_dir = os.path.join(FC_name, "Demultiplexing", project_name_underscore, "Sample_{}".format(sample_name))
            os.mkdir(sample_dir)
            sampinfo={ 'sampleid' : sample_name,
                    'received' : datetime.datetime.today().strftime("%Y-%m-%d"),
                    "total_autosomal_coverage" : "0",
                    "libs":{}
                    }
            library = "A" # only a single library
            sampinfo['libs'][library]={}
            sampinfo['libs'][library]['libprepid']=library
            sampinfo['libs'][library]['seqruns']={}
            
            sampinfo['libs'][library]['seqruns'][FC_name]={}
            sampinfo['libs'][library]['seqruns'][FC_name]['seqrunid']=FC_name
            sampinfo['libs'][library]['seqruns'][FC_name]['mean_autosomal_coverage']=0
            sampinfo['libs'][library]['seqruns'][FC_name]['sequencing_status']={1:"DONE"}
            sampinfo['libs'][library]['seqruns'][FC_name]['total_reads']=0
            # store sampinfo
            charon_data['samples'][sample_name]=sampinfo
            # now copy the fastq file
            fastq1_dest = os.path.join(sample_dir, "{}_S{}_L00{}_R1_001.fastq.gz".format(sample_name, lane, lane))
            fastq2_dest = os.path.join(sample_dir, "{}_S{}_L00{}_R2_001.fastq.gz".format(sample_name, lane, lane))
            if args.symlinks:
                os.symlink(args.fastq1, fastq1_dest)
                os.symlink(args.fastq2, fastq2_dest)
            else:
                shutil.copyfile(args.fastq1, fastq1_dest)
                shutil.copyfile(args.fastq2, fastq2_dest)
            
            sample_number += 1
    # now save everything to charon
    print "updaiting charon-dev"
    writeProjectData(charon_data, args.token, args.url)

    print "now run this for each FC"
    print "    python <path/to>/ngi_pipeline/scripts/ngi_pipeline_start.py organize <path/to/FC>"
    print "and then this to start the analysis"
    print "    python <path/to>/ngi_pipeline/scripts/ngi_pipeline_start.py analyze project<path/to>y/DATA/{}".format(project_identifier)




def touch(file):
    open(file, "w").close()
    
    
def create_sample_name(project_identifier, sample_number, facility):
    sample_id = str(sample_number).zfill(3)
    if facility == 'stockholm':
        return "{}_{}".format(project_identifier, sample_id)
    else:
        return "U{}".format(sample_id)
    
        



def writeProjectData(data, token, base_url):
    project=data
    samples=project.pop('samples', None)
    url=base_url+'/api/v1/project'
    projson=json.dumps(project)
    writeToCharon(projson, url, token)
    for sid in samples:
        libs=samples[sid].pop('libs', None)
        sampjson=json.dumps(samples[sid])
        url=base_url+'/api/v1/sample/'+project['projectid']
        writeToCharon(sampjson, url, token)
        for lib in libs:
            seqruns=libs[lib].pop('seqruns', None)
            libjson=json.dumps(libs[lib])
            url=base_url+'/api/v1/libprep/'+project['projectid']+"/"+sid
            writeToCharon(libjson, url, token)
            for seqrun in seqruns:
                seqjson=json.dumps(seqruns[seqrun])
                url=base_url+'/api/v1/seqrun/'+project['projectid']+"/"+sid+"/"+lib
                writeToCharon(seqjson, url, token)


def writeToCharon(jsonData, url, token):
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    r=session.post(url, headers=headers, data=jsonData)


def cleanCharon(pid, url, token):
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    r=session.delete(url+'/api/v1/project/'+pid, headers=headers)



def create_proj_name(project_name=None, facility='stockholm'):
    if project_name == None:
        if facility == 'stockholm':
            project_name_header = "J.Doe"
            year = generate_random_numbers(2)
            pnum = generate_random_numbers(2)
            project_name = "{}_{}_{}".format(project_name_header, year, pnum)
        else:
            project_name = "{}-{}".format(generate_random_chars(2), generate_random_numbers(4))
    else:
        project_name = project_name

    if facility == 'stockholm':
        project_id = "P{}".format(generate_random_numbers(3))
    else:
        project_id = project_name
    #TODO: CHECK THAT THE PROJECT DOES NOT EXISTS ALREADY
    return (project_name, project_id)


def create_fake_FC():
    # create something like 160217_ST-E00201_0063_AHJHNYCCXX
    new = False
    while (not new):
        # date
        date = generate_random_numbers(6)
        # instrument name
        instrument = generate_random_instrument()
        # run id
        run_id = generate_random_numbers(4)
        # FC position
        FC_pos = random.choice(["A", "B"])
        # FC name
        FC_name = generate_random_chars(9)
        # now compose the name
        run_name = "{}_{}_{}_{}{}".format(date, instrument, run_id, FC_pos, FC_name)
        # check if this FC exists already
        if not os.path.exists(run_name):
            # this FC does not exists, fine
            new = True
    return run_name


def generate_random_chars(chars=0):
    return ''.join(random.choice('ABCEFGHILMNOPQRSTUVZ') for i in xrange(chars))

def generate_random_numbers(numbers=0):
    return ''.join(str(random.randint(0,9)) for i in xrange(numbers))

def generate_random_instrument():
    instruments = ["ST-E00201", "ST-E00202", "ST-E00203", "ST-E00204", "ST-E00205"] # five instruments
    return  instruments[random.randint(0,4)]





if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This script generates a test suite for NGI-pipeline and piper testing. Needs as input two fastq files (read 1 and read 2) and creates as many FC as specified. The enviorment variables need to point to cahron-dev """)
    parser.add_argument('--fastq1', help="path to fastq file containing read 1", type=str,  required=True)
    parser.add_argument('--fastq2', help="path to fastq file containing read 2", type=str,  required=True)
    parser.add_argument('--FC', help="number of FC to be created", type=int,  required=True)
    parser.add_argument('--project-name', help="Name of the project that will be simulated (something like J.Doe_16_01)", type=str,
        default=None)
    parser.add_argument('--facility', help="facility sequencing the project (stkholm, uppsala)", type=str, default="stockholm",
        choices=('stockholm, uppsala'))
    parser.add_argument("-t", "--token", dest="token", default=os.environ.get('CHARON_API_TOKEN'),
            help="Charon API Token. Will be read from the env variable CHARON_API_TOKEN if not provided")
    parser.add_argument("-u", "--url", dest="url", default=os.environ.get('CHARON_BASE_URL'),
            help="Charon base url. Will be read from the env variable CHARON_BASE_URL if not provided")
    parser.add_argument("--delete", help="delete from charon the project id specified", type=str )
    parser.add_argument("--symlinks", help="instead of copying files creates symlinks", action='store_true', default=False)
    args = parser.parse_args()

    if not args.token :
        print( "No valid token found in arg or in environment. Exiting.")
        sys.exit(-1)
    if not args.url:
        print( "No valid url found in arg or in environment. Exiting.")
        sys.exit(-1)

    if 'dev' not in args.url:
        print( "Something tells me that you not using charon-dev.... {}".format(args.url))
        sys.exit(-1)


    main(args)




