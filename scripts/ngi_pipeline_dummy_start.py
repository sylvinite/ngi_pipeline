import argparse
import time


from ngi_pipeline import conductor
from ngi_pipeline.conductor import check_update_jobs_status

def main(demux_fcid_dirs, test_step_1, test_step_2, restrict_to_projects=None, restrict_to_samples=None):
    if not test_step_1 and not test_step_2:
        conductor.process_demultiplexed_flowcells(demux_fcid_dirs, restrict_to_projects, restrict_to_samples)
    elif test_step_1:
        print "Extensive testing"
        
        check_update_jobs_status()
        
        demux_fcid_dirs = ["/proj/a2010002/INBOX/140528_D00415_0049_BC423WACXX"] # G.Grigelioniene_14_01
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this should look like a typical command trigger by the Celery server
        
        
        print "now waiting for 1 minute ..."
        time.sleep(60) #wait for 5 minutes
        check_update_jobs_status()
        
        #FOR NOW SKIP THIS HAS IT SEEMS TO FAILS
        #demux_fcid_dirs = ["/proj/a2010002/INBOX/140702_D00415_0052_AC41A2ANXX"] # M.Kaller_14_06
        #conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this should look like a typical command trigger by the Celery server
        
        print "now waiting for 10 minutes ..."
        time.sleep(60) #wait for 5 minutes
        
        demux_fcid_dirs = ["/proj/a2010002/INBOX/130611_SN7001298_0148_AH0CCVADXX/"] #A.Wedell_13_03 sample P567_101 same run
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None)
        
        print "now waiting for 10 minutes ..."
        time.sleep(600) #wait for 5 minutes
        
        
        demux_fcid_dirs =["/proj/a2010002/INBOX/130611_SN7001298_0148_AH0CCVADXX/"] # Same as above, this must fail or not be processed
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this should look like a typical command trigger by the Celery server
        
        print "now waiting for 10 minutes ..."
        time.sleep(600) #wait for 5 minutes
       
        demux_fcid_dirs = ["/proj/a2010002/INBOX/130612_D00134_0019_AH056WADXX/"] # second flowcell arrives with 10 minutes delay
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this must start

        print "now waiting for 10 minutes ..."
        time.sleep(600) #wait for 5 minutes
        
        demux_fcid_dirs = ["/proj/a2010002/INBOX/130627_D00134_0023_AH0JYUADXX/"]
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this must start

        print "now waiting for 10 minutes ..."
        time.sleep(600) #wait for 5 minutes
        
        demux_fcid_dirs = ["/proj/a2010002/INBOX/130701_SN7001298_0152_AH0J92ADXX/"]
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this must start
        
        
        print "now waiting for 10 minutes ..."
        time.sleep(600) #wait for 5 minutes
        demux_fcid_dirs = ["/proj/a2010002/INBOX/130701_SN7001298_0153_BH0JMGADXX/"]
        conductor.process_demultiplexed_flowcell(demux_fcid_dirs, None, None) # this must start

        #and now a loop to update the DB
        while True:
            check_update_jobs_status()
            time.sleep(600)

    else:
        print "testing A.Wedell variant calling"

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Quick launcher for testing purposes.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects.")) #            default=["G.Grigelioniene_14_01"],

    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("demux_fcid_dirs", nargs="*", action="store",
            default=["/proj/a2010002/nobackup/mario/DATA/140528_D00415_0049_BC423WACXX/"],
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    parser.add_argument("-t1", "--test_step_1", dest= "test_step_1",  action='store_true', default=False,
            help=("Process one run per time of A.Wedell project "))

    parser.add_argument("-t2", "--test_step_2", dest= "test_step_2", action='store_true', default=False,
            help=("Checks if A.Wedell is ready to be analysed further"))


    args_dict = vars(parser.parse_args())
    main(**args_dict)


"""
project A.Wedell_13_03 is well suited for testing
A.Wedell_13_03
/proj/a2010002/INBOX/130611_SN7001298_0148_AH0CCVADXX/    --> P567_101
/proj/a2010002/INBOX/130612_D00134_0019_AH056WADXX/       --> P567_101
/proj/a2010002/INBOX/130627_D00134_0023_AH0JYUADXX/       --> P567_102
/proj/a2010002/INBOX/130701_SN7001298_0152_AH0J92ADXX/    --> P567_102
/proj/a2010002/INBOX/130701_SN7001298_0153_BH0JMGADXX/    --> P567_102

G.Grigelioniene_14_01
/proj/a2010002/INBOX/140528_D00415_0049_BC423WACXX        --> P1142_101

M.Kaller_14_06
/proj/a2010002/INBOX/140702_D00415_0052_AC41A2ANXX        --> P1171_102  P1171_104  P1171_106  P1171_108





"""