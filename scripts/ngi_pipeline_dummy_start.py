"""
project A.Wedell_13_03 is well suited for testing
A.Wedell_13_03
/proj/a2014205/INBOX/130611_SN7001298_0148_AH0CCVADXX/    --> P567_101
/proj/a2014205/INBOX/130612_D00134_0019_AH056WADXX/       --> P567_101
/proj/a2014205/INBOX/130627_D00134_0023_AH0JYUADXX/       --> P567_102
/proj/a2014205/INBOX/130701_SN7001298_0152_AH0J92ADXX/    --> P567_102
/proj/a2014205/INBOX/130701_SN7001298_0153_BH0JMGADXX/    --> P567_102
G.Grigelioniene_14_01
/proj/a2014205/INBOX/140528_D00415_0049_BC423WACXX        --> P1142_101
M.Kaller_14_06
/proj/a2014205/INBOX/140702_D00415_0052_AC41A2ANXX        --> P1171_102


"""

import argparse
import time
import os

from ngi_pipeline.conductor.flowcell import process_demultiplexed_flowcell, process_demultiplexed_flowcells
from ngi_pipeline.conductor.launchers import launch_analysis_for_samples
#from ngi_pipeline.database.local_process_tracking import check_update_jobs_status


def main(demux_fcid_dir, restrict_to_projects=None, restrict_to_samples=None):

        import pdb
        pdb.set_trace()
        
        
        demux_fcid_dir = "/proj/a2014205/INBOX/130611_SN7001298_0148_AH0CCVADXX/" #A.Wedell_13_03 sample P567_101
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2014205/INBOX/130612_D00134_0019_AH056WADXX/" # A.Wedell_13_03 sample P567_101
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2014205/INBOX/130627_D00134_0023_AH0JYUADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2014205/INBOX/130701_SN7001298_0152_AH0J92ADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes

        demux_fcid_dir = "/proj/a2014205/INBOX/130701_SN7001298_0153_BH0JMGADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        
        demux_fcid_dir = "/proj/a2014205/INBOX/140528_D00415_0049_BC423WACXX" # G.Grigelioniene_14_01
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2014205/INBOX/140702_D00415_0052_AC41A2ANXX" # M.Kaller_14_06 sample P1171_102, P1171_104, P1171_106, P1171_108
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        
        demux_fcid_dir = "/proj/a2014205/INBOX/140905_D00415_0057_BC45KVANXX" # M.Kaller_14_06 sample P1171_102, P1171_104, P1171_106 ---- rerun
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes
        
        
        demux_fcid_dir = "/proj/a2014205/INBOX/140815_SN1025_0222_AC4HA6ACXX" # M.Kaller_14_05 sample P1170_101, P1170_103, P1170_105
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)            # M.Kaller_14_08 sample P1272_101, P1272_104
        time.sleep(60) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2014205/INBOX/140815_SN1025_0223_BC4HAPACXX" # M.Kaller_14_05 sample P1170_101, P1170_103, P1170_105
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)            # M.Kaller_14_08 sample P1272_101, P1272_104
        time.sleep(60) #wait for 1 minutes
        
        
        demux_fcid_dir = "/proj/a2014205/INBOX/140919_SN1018_0203_BHA3THADXX" # M.Kaller_14_05  P1170_103, P1170_105  --- rerun
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(60) #wait for 1 minutes




        ###UPPSALA
        #/proj/a2010002/INBOX/140821_D00458_0029_AC45JGANXX

        #check_update_jobs_status()
        #trigger_sample_level_analysis()
        
        #for demux_fcid in ("146362_3YUPS4_4614_AYSD4DN8PK", "382000_FA15AC_1819_ADT0SZ8CAV", "526414_H4VQMR_0554_A2Z2D30IMP", "636430_FHTLAG_5491_ADVN76RAX0", "694405_SGXBZU_8447_ANAY949DA1", "707581_O81L8Q_5350_ABBQEYRQQR", "958288_QH3TH9_2625_AOET9TT9AK", "992290_KV8699_9429_AJM8N4NV1U"):
        #    demux_fcid_dir= os.path.join("/proj/a2014205/INBOX/", demux_fcid)
        #    process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        #    time.sleep(30)
        
        
        #and now a loop to update the DB
        #while True:
        #    check_update_jobs_status()
        #    trigger_sample_level_analysis()
        #    #check status every half an hour
        #    time.sleep(3800)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Quick launcher for testing purposes.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    parser.add_argument("demux_fcid_dir", nargs="?", action="store",
            default="/proj/a2014205/nobackup/mario/DATA/140528_D00415_0049_BC423WACXX/",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    args_dict = vars(parser.parse_args())
    main(**args_dict)
