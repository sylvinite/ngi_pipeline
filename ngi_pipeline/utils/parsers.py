import collections
import csv
import glob
import os
import re
import shlex
import subprocess
import xml.etree.cElementTree as ET
import xml.parsers.expat

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import memoized

LOG = minimal_logger(__name__)

## e.g. A.Wedell_13_03_P567_102_A_140528_D00415_0049_BC423WACXX                         <-- sthlm
##  or  ND-0522_NA10860_PCR-free_SX398_NA10860_PCR-free_140821_D00458_0029_AC45JGANXX   <-- uusnp
STHLM_UUSNP_SEQRUN_RE = re.compile(r'(?P<project_name>\w\.\w+_\d+_\d+|\w{2}-\d+)_(?P<sample_id>[\w-]+)_(?P<libprep_id>\w|\w{2}\d{3}_\2)_(?P<seqrun_id>\d{6}_\w+_\d{4}_.{10})')
STHLM_UUSNP_SAMPLE_RE = re.compile(r'(?P<project_name>\w\.\w+_\d+_\d+|\w{2}-\d+)_(?P<sample_id>[\w-]+)')



SLURM_EXIT_CODES = {"PENDING": None,
                    "RUNNING": None,
                    "RESIZING": None,
                    "SUSPENDED": None,
                    "COMPLETED": 0,
                    "CANCELLED": 1,
                    "FAILED": 1,
                    "TIMEOUT": 1,
                    "PREEMPTED": 1,
                    "BOOT_FAIL": 1,
                    "NODE_FAIL": 1,
                    }

def get_slurm_job_status(slurm_job_id):
    """Gets the State of a SLURM job and returns it as an integer (or None).

    :param int slurm_job_id: An integer of your choosing

    :returns: The status of the job (None == Queued/Running, 0 == Success, 1 == Failure)
    :rtype: None or int

    :raises TypeError: If the input is not/cannot be converted to an int
    :raises ValueError: If the slurm job ID is not found
    :raises RuntimeError: If the slurm job status is not understood
    """
    try:
        check_cl = "sacct -n -j {:d} -o STATE".format(slurm_job_id)
        # If the sbatch job has finished, this returns two lines. For example:
        # $ sacct -j 3655032
        #       JobID    JobName  Partition    Account  AllocCPUS      State ExitCode 
        #       ------------ ---------- ---------- ---------- ---------- ---------- -------- 
        #       3655032      test_sbat+       core   a2010002          1  COMPLETED      0:0 
        #       3655032.bat+      batch              a2010002          1  COMPLETED      0:0 
        #
        # In this case I think we want the first one but I'm actually still not
        # totally clear on this point -- the latter may be the return code of the
        # actual sbatch command for the bash interpreter? Unclear.
    except ValueError:
        raise TypeError("SLURM Job ID not an integer: {}".format(slurm_job_id))
    job_status = subprocess.check_output(shlex.split(check_cl))
    if not job_status:
        raise ValueError("No such slurm job found: {}".format(slurm_job_id))
    else:
        try:
            return SLURM_EXIT_CODES[job_status.split()[0].strip("+")]
        except (IndexError, KeyError, TypeError) as e:
            raise RuntimeError("SLURM job status not understood: {}".format(job_status))


def slurm_time_to_seconds(slurm_time_str):
    """Convert a time in a normal goddamned format into seconds.
    Must follow the format:
        days-hours:minutes:seconds
    e.g.
        0-12:34:56
    or else I will just return 4 days and that's what you get for getting
    cute about the formatting.
    """
    try:
        days, time = slurm_time_str.split("-")
        hours, minutes, seconds = map(int, time.split(":"))
        hours += int(days) * 24
        minutes += hours * 60
        seconds += minutes * 60
    except Exception as e:
        LOG.error('Couldn\'t parse passed time "{}": {}'.format(slurm_time_str, e))
        return 345600
    return seconds


def determine_library_prep_from_fcid(project_id, sample_name, fcid):
    """Use the information in the database to get the library prep id
    from the project name, sample name, and flowcell id.

    :param str project_id: The ID of the project
    :param str sample_name: The name of the sample
    :param str fcid: The flowcell ID

    :returns: The library prep (e.g. "A")
    :rtype str
    :raises ValueError: If no match was found.
    """
    charon_session = CharonSession()
    try:
        libpreps = charon_session.sample_get_libpreps(project_id, sample_name)['libpreps']
        if libpreps:
            for libprep in libpreps:
                # Get the sequencing runs and see if they match the FCID we have
                seqruns = charon_session.libprep_get_seqruns(project_id,
                                                             sample_name,
                                                             libprep['libprepid'])['seqruns']
                if seqruns:
                    for seqrun in seqruns:
                        seqrun_runid = seqrun["seqrunid"]
                        if seqrun_runid == fcid:
                            return libprep['libprepid']
                    else:
                        raise CharonError("No match", 404)
                else:
                    raise CharonError("No seqruns found!", 404)
        else:
            raise CharonError("No libpreps found!", 404)
    except CharonError as e:
        if e.status_code == 404:
            raise ValueError('No library prep found for project "{}" / sample "{}" '
                             '/ fcid "{}"'.format(project_id, sample_name, fcid))
        else:
            raise ValueError('Could not determine library prep for project "{}" '
                             '/ sample "{}" / fcid "{}": {}'.format(project_id,
                                                                    sample_name,
                                                                    fcid,
                                                                    e))


def determine_libprep_from_uppsala_samplesheet(samplesheet_path, project_id, sample_id, seqrun_id, lane_num):
    samplesheet = parse_samplesheet(samplesheet_path)
    fcid = seqrun_id.split("_")[3][1:]
    for row in samplesheet:
        ss_project_id = row["SampleProject"]
        ss_sample_id = row["SampleID"]
        ss_fcid = row["FCID"]
        ss_lane_num = int(row["Lane"])

        if project_id == ss_project_id and \
           sample_id == ss_sample_id and \
           fcid == ss_fcid and \
           lane_num == ss_lane_num:
               # Resembles 'LIBRARY_NAME:SX398_NA11993_Nano'
               try:
                   return row["Description"].split(":")[1]
               except IndexError:
                   error_msg = ('Malformed description in "{}"; cannot get '
                                'libprep information'.format(samplesheet_path))
                   LOG.warn(error_msg)
                   raise ValueError(error_msg)
    error_msg = ('No match found in "{}" for project "{}" / sample "{}" / '
                 'seqrun "{}" / lane number "{}"'.format(samplesheet_path,
                                                         project_id, sample_id,
                                                         seqrun_id, lane_num))
    LOG.warn(error_msg)
    raise ValueError(error_msg)


@memoized
def parse_samplesheet(samplesheet_path):
    """Parses an Illumina SampleSheet.csv and returns a list of dicts
    """
    with open(samplesheet_path, 'rU') as f:
        return [ row for row in csv.DictReader(f, dialect="excel") ]


def find_fastq_read_pairs_from_dir(directory):
    """
    Given the path to a directory, finds read pairs (based on _R1_/_R2_ file naming)
    and returns a dict of {base_name: [ file_read_one, file_read_two ]}
    Filters out files not ending with .fastq[.gz|.gzip|.bz2].

    E.g. a path to a directory containing:
        P567_102_AAAAAA_L001_R1_001.fastq.gz
        P567_102_AAAAAA_L001_R2_001.fastq.gz
    becomes
        { "P567_102_AAAAAA_L001":
           ["P567_102_AAAAAA_L001_R1_001.fastq.gz",
            "P567_102_AAAAAA_L001_R2_001.fastq.gz"] }

    :param str directory: The directory to search for fastq file pairs.
    :returns: A dict of file_basename -> [file1, file2]
    :rtype: dict
    """
    file_list = glob.glob(os.path.join(directory, "*"))
    return find_fastq_read_pairs(file_list)


def find_fastq_read_pairs(file_list):
    """
    Given a list of file names, finds read pairs (based on _R1_/_R2_ file naming)
    and returns a dict of {base_name: [ file_read_one, file_read_two ]}
    Filters out files not ending with .fastq[.gz|.gzip|.bz2].
    E.g.
        P567_102_AAAAAA_L001_R1_001.fastq.gz
        P567_102_AAAAAA_L001_R2_001.fastq.gz
    becomes
        { "P567_102_AAAAAA_L001":
           ["P567_102_AAAAAA_L001_R1_001.fastq.gz",
            "P567_102_AAAAAA_L001_R2_001.fastq.gz"] }

    :param list file_list: A list of files in no particular order

    :returns: A dict of file_basename -> [file1, file2]
    :rtype: dict
    """
    # We only want fastq files
    pt = re.compile(".*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$")
    file_list = filter(pt.match, file_list)
    if not file_list:
        # No files found
        LOG.warn("No fastq files found.")
        return {}
    # --> This is the SciLifeLab-Sthlm-specific format (obsolete as of August 1st, hopefully)
    #     Format: <lane>_<date>_<flowcell>_<project-sample>_<read>.fastq.gz
    #     Example: 1_140220_AH8AMJADXX_P673_101_1.fastq.gz
    # --> This is the standard Illumina/Uppsala format (and Sthlm -> August 1st 2014)
    #     Format: <sample_name>_<index>_<lane>_<read>_<group>.fastq.gz
    #     Example: NA10860_NR_TAAGGC_L005_R1_001.fastq.gz
    suffix_pattern = re.compile(r'(.*)fastq')
    # Cut off at the read group
    file_format_pattern = re.compile(r'(.*)_(?:R\d|\d\.).*')
    matches_dict = collections.defaultdict(list)
    for file_pathname in file_list:
        file_basename = os.path.basename(file_pathname)
        try:
            # Check for a pair
            pair_base = file_format_pattern.match(file_basename).groups()[0]
            matches_dict[pair_base].append(file_pathname)
        except AttributeError:
            LOG.warn("Warning: file doesn't match expected file format, "
                      "cannot be paired: \"{}\"".format(file_fullname))
            # File could not be paired, set by itself (?)
            file_basename_stripsuffix = suffix_pattern.split(file_basename)[0]
            matches_dict[file_basename_stripsuffix].append(os.abspath(file_fullname))
    return dict(matches_dict)


def parse_lane_from_filename(sample_basename):
    """Lane number is parsed from the standard filename format,
     which is one of:
       <sample-name>_<index>_<lane>_<read>_<group>.fastq.gz
       e.g.
       P567_102_AAAAAA_L001_R1_001.fastq.gz
       (Standard Illumina format)
    or
       <lane_num>_<date>_<fcid>_<project>_<sample_num>_<read>.fastq[.gz]
       e.g.
       1_140220_AH8AMJADXX_P673_101_1.fastq.gz
       (SciLifeLab Sthlm format, obsolete)

    returns a lane as an int or raises a ValueError if there is no match
    (which shouldn't generally happen and probably indicates a larger issue).

    :param str sample_basename: The name of the file from which to pull the project id
    :returns: (project_id, sample_id)
    :rtype: tuple
    :raises ValueError: If the ids cannot be determined from the filename (no regex match)
    """
    # Stockholm or \
    # Illumina
    match = re.match(r'(?P<lane>\d)_\d{6}_\w{10}_(?P<project>P\d{3})_(?P<sample>\d{3}).*', sample_basename) or \
            re.match(r'.*_L\d{2}(?P<lane>\d{1}).*', sample_basename)
            #re.match(r'(?P<project>P\d{3})_(?P<sample>\w+)_.*_L(?P<lane>\d{3})', sample_basename)

    if match:
        #return match.group('project'), match.group('sample'), match.group('lane')
        return int(match.group('lane'))
    else:
        error_msg = ('Error: filename didn\'t match conventions, '
                     'couldn\'t find lane number for sample '
                     '"{}"'.format(sample_basename))
        LOG.error(error_msg)
        raise ValueError(error_msg)


@memoized
def get_flowcell_id_from_dirtree(path):
    """Given the path to a file, tries to work out the flowcell ID.

    Project directory structure is generally either:
        <run_id>/Sample_<project-sample-id>/
         131018_D00118_0121_BC2NANACXX/Sample_NA10860_NR/
        (Uppsala format)
    or:
        <project>/<project-sample-id>/<libprep>/<date>_<flowcell>/
        J.Doe_14_03/P673_101/140220_AH8AMJADXX/
        (NGI format)
    :param str path: The path to the file
    :returns: The flowcell ID
    :rtype: str
    :raises ValueError: If the flowcell ID cannot be determined
    """
    flowcell_pattern = re.compile(r'\d{4,6}_(?P<fcid>[A-Z0-9]{10})')
    try:
        # NGI format (4-dir)
        path, dirname = os.path.split(path)
        return flowcell_pattern.match(dirname).groups()[0]
    except (IndexError, AttributeError):
        try:
            # SciLifeLab Uppsala tree format (2-dir)
            _, dirname = os.path.split(path)
            return flowcell_pattern.match(dirname).groups()[0]
        except (IndexError, AttributeError):
            raise ValueError("Could not determine flowcell ID from directory path.")


class XmlToList(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlToDict(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlToList(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)
            else:
                # Set dict for attributes
                self.append({k:v for k,v in element.items()})


# Sometimes you don't want to deal with a goddamn object you just want your goddamn XML dict
def xmltodict_file(config_file):
    tree = ET.parse(config_file)
    root = tree.getroot()
    return XmlToDict(root)


# Generic XML to dict parsing
# See http://code.activestate.com/recipes/410469-xml-as-dictionary/
class XmlToDict(dict):
    '''
    Example usage:

    >>> tree = ET.parse('your_file.xml')
    >>> root = tree.getroot()
    >>> xmldict = XmlToDict(root)

    Or, if you want to use an XML string:

    >>> root = ET.XML(xml_string)
    >>> xmldict = XmlToDict(root)

    And then use xmldict for what it is... a dict.
    '''
    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if element:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlToDict(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself
                    aDict = {element[0].tag: XmlToList(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            ## additional note from years later, nobody ever comes back to look at old code and examine assumptions
            elif element.items():
                self.update({element.tag: dict(element.items())})
                # add the following line
                self[element.tag].update({"__Content__":element.text})

            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})


class RunMetricsParser(dict):
    """Generic Run Parser class"""
    _metrics = []
    ## Following paths are ignored
    ignore = "|".join(["tmp", "tx", "-split", "log"])
    reignore = re.compile(ignore)

    def __init__(self, log=None):
        super(RunMetricsParser, self).__init__()
        self.files = []
        self.path=None
        self.log = LOG
        if log:
            self.log = log

    def _collect_files(self):
        if not self.path:
            return
        if not os.path.exists(self.path):
            raise IOError
        self.files = []
        for root, dirs, files in os.walk(self.path):
            if re.search(self.reignore, root):
                continue
            self.files = self.files + [os.path.join(root, x) for x in files]

    def filter_files(self, pattern, filter_fn=None):
        """Take file list and return those files that pass the filter_fn criterium"""
        def filter_function(f):
            return re.search(pattern, f) != None
        if not filter_fn:
            filter_fn = filter_function
        return filter(filter_fn, self.files)

    def parse_json_files(self, filter_fn=None):
        """Parse json files and return the corresponding dicts
        """
        def filter_function(f):
            return f is not None and f.endswith(".json")
        if not filter_fn:
            filter_fn = filter_function
        files = self.filter_files(None,filter_fn)
        dicts = []
        for f in files:
            with open(f) as fh:
                dicts.append(json.load(fh))
        return dicts

    def parse_csv_files(self, filter_fn=None):
        """Parse csv files and return a dict with filename as key and the corresponding dicts as value
        """
        def filter_function(f):
            return f is not None and f.endswith(".csv")
        if not filter_fn:
            filter_fn = filter_function
        files = self.filter_files(None,filter_fn)
        dicts = {}
        for f in files:
            with open(f) as fh:
                dicts[f] = [r for r in csv.DictReader(fh)]
        return dicts


class RunInfoParser():
    """RunInfo parser"""
    def __init__(self):
        self._data = {}
        self._element = None

    def parse(self, fp):
        self._parse_RunInfo(fp)
        return self._data

    def _start_element(self, name, attrs):
        self._element=name
        if name == "Run":
            self._data["Id"] = attrs["Id"]
            self._data["Number"] = attrs["Number"]
        elif name == "FlowcellLayout":
            self._data["FlowcellLayout"] = attrs
        elif name == "Read":
            self._data["Reads"].append(attrs)

    def _end_element(self, name):
        self._element=None

    def _char_data(self, data):
        want_elements = ["Flowcell", "Instrument", "Date"]
        if self._element in want_elements:
            self._data[self._element] = data
        if self._element == "Reads":
            self._data["Reads"] = []

    def _parse_RunInfo(self, fp):
        p = xml.parsers.expat.ParserCreate()
        p.StartElementHandler = self._start_element
        p.EndElementHandler = self._end_element
        p.CharacterDataHandler = self._char_data
        p.ParseFile(fp)


class RunParametersParser():
    """runParameters.xml parser"""
    def __init__(self):
        self.data = {}

    def parse(self, fh):
        tree = ET.parse(fh)
        root = tree.getroot()
        self.data = XmlToDict(root)
        # If not a MiSeq run, return the contents of the Setup tag
        if 'MCSVersion' not in self.data:
            self.data = self.data['Setup']
        return self.data


class FlowcellRunMetricsParser(RunMetricsParser):
    """Flowcell level class for parsing flowcell run metrics data."""
    def __init__(self, path):
        RunMetricsParser.__init__(self)
        self.path = path

    def parseRunInfo(self, fn="RunInfo.xml", **kw):
        infile_path = os.path.join(os.path.abspath(self.path), fn)
        self.log.info("Reading run info from file {}".format(infile_path))
        with open(infile_path, 'r') as f:
            parser = RunInfoParser()
            data = parser.parse(f)
        return data

    def parseRunParameters(self, fn="runParameters.xml", **kw):
        """Parse runParameters.xml from an Illumina run.

        :param fn: filename
        :param **kw: keyword argument

        :returns: parsed data structure
        """
        infile_path = os.path.join(os.path.abspath(self.path), fn)
        self.log.info("Reading run parameters from file {}".format(infile_path))
        with open(infile_path) as f:
            parser = RunParametersParser()
            data = parser.parse(f)
        return data

## TODO Pythonify this up a bit
def parse_qualimap_results(qualimap_results_path):
    """Parse the genome_results.txt file created by Piper (qualimap).

    :param str qualimap_results_path: The path to the Qualimap results file to be parsed.
    
    :returns: A dictionary of metrics of interest
    :rtype: dict
    :raises IOError: If the qualimap_results_path file cannot be opened for reading.
    """
    with open(qualimap_results_path, 'r') as fh:
        current_flag=None
        data={}
        cfp=re.compile('>>>>>>> ([^\n]+)')
        #cov=re.compile('There is a ([0-9\.%]+) of reference with a coverageData >= ([0-9X]+)')
        ccp=re.compile('^\s*[0-9]{1,2}\s[0-9]+\s[0-9]+\s([0-9\.]+)\s[0-9]+')
        mac=0
        for line in fh.readlines():
            if line:
                if ">>>>>>>" in line:
                    current_flag=cfp.search(line).group(1)
                    data[current_flag]={}
                else:
                    keyval=line.split(" = ")
                    if len(keyval)==2:
                        key=re.sub('^\s+', '', keyval[0])
                        key=re.sub('\n', '', key)
                        val=re.sub('^\s+', '', keyval[1])
                        val=re.sub('\n', '', val)
                        data[current_flag][key]=val

                    #if current_flag=="Coverage" and cov.search(line): 
                    #    data[current_flag][cov.search(line).group(2)]=cov.search(line).group(1)

                    if current_flag=="Coverage per contig" and ccp.search(line):
                        mac+=float(ccp.search(line).group(1))
#postprocess : remove useless stuff
        mac/=22
        data['mean_autosomal_coverage']=mac
        data['mean_coverage']=data['Coverage']['mean coverageData']


        del(data['Coverage per contig'])
        data['GC percentage']=data['ACTG content']['GC percentage']
        data['std_coverage']=data['Coverage']['std coverageData']
        data['aligned_bases']=data['Globals']['number of aligned bases']
        data['mapped_bases']=data['Globals']['number of mapped bases']
        data['mapped_reads']=data['Globals']['number of mapped reads']
        data['reads_per_lane']=data['Globals']['number of reads']
        data['sequenced_bases']=data['Globals']['number of sequenced bases']
        data['bam_file']=data['Input']['bam file']
        data['output_file']=data['Input']['outfile']
    
        data['GC_percentage']=data['GC percentage']
        data['mean_mapping_quality']=data['Mapping quality']['mean mapping quality']
        data['bases_number']=data['Reference']['number of bases']
        data['contigs_number']=data['Reference']['number of contigs']

        data['windows']=data['Globals']['number of windows']

        del data['ACTG content']
        del data['Coverage']['mean coverageData']
        del data['Coverage']['std coverageData']
        del data['Coverage']
        del(data['Globals'])
        del(data['Input'])
        del data['GC percentage']
        del(data['Reference'])
        del(data['Mapping quality'])

#mean autosome coverage"
#number of duplicates
        return data
