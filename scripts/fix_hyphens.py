
import glob
import os
import argparse
import re

def main(args):

    pattern=re.compile("(P[0-9]+)-([0-9]{3,4})")
    for root, dirs, files in os.walk(args.path):
        for dir in dirs :
            matches=pattern.match(dir)
            if matches:
                replacement=dir.replace("{}-{}".format(matches.group(1), matches.group(2)), "{}_{}".format(matches.group(1),matches.group(2)))
                print "replacing {} \nwith {}".format(os.path.join(root,dir), os.path.join(root,replacement))
                os.rename(os.path.join(root,dir), os.path.join(root,replacement))
        for file in files:
            matches=pattern.match(file)
            if matches:
                replacement=file.replace("{}-{}".format(matches.group(1), matches.group(2)), "{}_{}".format(matches.group(1), matches.group(2)))
                print "replacing {} \nwith {}".format(os.path.join(root,file), os.path.join(root,replacement))
                os.rename(os.path.join(root,file), os.path.join(root,replacement))






if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', '-p',dest='path',required=True,help='path of the folder to work on')
    args=parser.parse_args()
    main(args)
