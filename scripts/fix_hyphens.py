
import glob
import os
import argparse

def main(args):

    project=os.path.basename(args.path)
    for samplef in glob.glob(os.path.join(args.path,'*')):
        sample=os.path.basename(samplef).split('_')[1]
        for libf in glob.glob(os.path.join(samplef, '*')):
            for runf in glob.glob(os.path.join(libf, '*')):
                for file in glob.glob(os.path.join(runf, '{}-{}*'.format(project, sample))):
                    print "working on"+file
                    if os.path.islink(file):
                        import pdb;pdb.set_trace()
                        new_name="{}_{}{}".format(project,sample,os.path.basename(file).split(sample)[1])
                        path=os.readlink(file);
                        os.unlink(file)
                        os.symlink(path, os.path.join(runf, new_name))




if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', '-p',dest='path',required=True,help='path of the folder to work on')
    args=parser.parse_args()
    main(args)
