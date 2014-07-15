from ngi_pipeline import conductor


def main():
    conductor.process_demultiplexed_flowcells(["/proj/a2010002/nobackup/mario/DATA/140528_D00415_0049_BC423WACXX/"], restrict_to_projects=["G.Grigelioniene_14_01"])
    return 1

if __name__ == '__main__':
    main()
