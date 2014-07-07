import os
import random
import re
import string
import tempfile


def generate_project_name():
    """Generate a project name in the form Y.Mom_12_34
    """
    return "{}.{}_{}_{}".format(
            random.choice(string.ascii_uppercase),
            "".join([random.choice(string.ascii_lowercase) for i in xrange(6) ]).capitalize(),
            random.randint(12,14),
            random.randint(11,99))

def generate_sample_name(project_id=random.randint(100,999)):
    """Generate a sample name in the form P123_456
    """
    return "P{}_{}".format(project_id, random.randint(101,199))


def generate_flowcell_id():
    """Generate a flowcell barcode in the format ABC123CXX
    """
    return "{}CXX".format("".join([random.choice(string.ascii_uppercase, string.digits) for i in xrange(6)]))


def generate_barcode(length=6):
    """Generate a nucleotide barcode
    """
    return generate_nucleotide_sequence(seq_length=length)


def generate_nucleotide_sequence(seq_length, alphabet="AGCT"):
    """Generate a random string of specified length ahd nucleotides sampled
    from the specified alphabet
    """
    if not alphabet:
        alphabet = "AGCT"
    return "".join([ random.choice(alphabet) for i in xrange(seq_length) ])


def generate_sample_file_name(sample_name=generate_sample_name(),
                              barcode=generate_barcode(),
                              lane=random.randint(1,8),
                              read_num=1):
    """Generate a CASAVA 1.8+-style sample file name in the format
    P567_102_AAAAAA_L001_R1_001.fastq.gz
    """
    return "{project_sample}_{barcode}_{lane}_R{read_num}_001.fastq.gz".format(**locals())


def generate_run_id(date=datetime.date.today().strformat("%y%m%d"),
                    instrument_id=generate_instrument_id(),
                    fcid=generate_flowcell_id()):
    """Generate a run identifier in the format:
        <YYMMDD>_<instrument_id>_0<nnn>_<FCID>
    """
    return "{date}_{instrument_id}_0{number}_{A_B}{fcid}".format(date=date,
                                                              instrument_id=instrument_id,
                                                              number=random.randint(101,999),
                                                              A_B=random.choice("AB"),
                                                              fcid=fcid)


def generate_identifier(date=datetime.date.today().strftime("%y%m%d"),
                        flowcell_barcode=generate_flowcell_barcode()):
    """Generate a date_barcode identifier in the format 140704_ABC123CXX
    """
    return "{}_{}".format(date, flowcell_barcode)


def create_demultiplexed_flowcell():
    """
    140528_D00415_0049_BC423WACXX/
    ├── C423WACXX.csv
    ├── RTAComplete.txt
    ├── RunInfo.xml
    ├── runParameters.xml
    ├── SampleSheet_0bp.csv
    ├── SampleSheet_8bp.csv
    ├── second_read_processing_started.txt
    ├── Unaligned
    │   └── Basecall_Stats_C423WACXX
    ├── Unaligned_0bp
    │   ├── Basecall_Stats_C423WACXX
    │   ├── DemultiplexConfig.xml
    │   ├── DemultiplexedBustardConfig.xml
    │   ├── DemultiplexedBustardSummary.xml
    │   ├── Project_G__Grigelioniene_14_01
    │   ├── Temp
    │   └── Undetermined_indices
    └── Unaligned_8bp
        ├── Basecall_Stats_C423WACXX
        ├── DemultiplexConfig.xml
        ├── DemultiplexedBustardConfig.xml
        ├── DemultiplexedBustardSummary.xml
        ├── Project_M__Nister_14_01
        ├── Temp
        └── Undetermined_indices
    """
    # Need at least: RunInfo.xml, runParameters.xml, Unaligned*, SampleSheet.csv?
    #   Unaligned/Basecall_Stats_*, Undetermined_Indices?,
    # Project/Sample/[fq1, fq2, ..., fqn]
    run_id = generate_run_id()
    project_name = generate_project_name()
    sample_name = generate_sample_name()
    run_info_xml_text = generate_RunInfo()


def generate_RunInfo(run_id=None, fcid=generate_flowcell_id(),
                     date=datetime.date.today().strftime("%y%m%d"),
                     instrument_id=generate_instrument_id()):
    """Generate a dummy RunInfo.xml file. This contains only the "Flowcell",
    "Date", and "Instrument" parameters.
    """
    run_id = generate_run_id(date=date, instrument_id=instrument_id, fcid=fcid)
    if run_id:  # User submitted a run id which we must parse
        try:
            date, instrument_id, _, fcid = run_id.split("_")
        except ValueError as e:
            raise ValueError("Improperly formatted Run ID: should be in the form "
                             "<date>_<instrument_id>_<nnnn>_<A/B+FCID>")
    else:   # We make our own rules
        date=datetime.date.today()
        instrument_id=generate_instrument_id()
        fcid=generate_flowcell_id()
        run_id = generate_run_id(date=date, instrument_id=instrument_id, fcid=fcid)

    # I suppose it would be better here to make some kind of dict->xml function
    # So you feel free to go ahead and write that for me
    return (
    '<?xml version="1.0">'
    '<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="2">'
    '  <Run Id="{run_id}" Number="01">'
    '    <Flowcell>{fcid}</Flowcell>'
    '    <Instrument>{instrument_id}</Instrument>'
    '    <Date>{date}</Date>'
    '  </Run>'
    '</RunInfo>').format(run_id=run_id, fcid=fcid, instrument_id=instrument_id,
                         date=date)


def create_project_structure(project_name=generate_project_name(),
                               run_id=generate_run_id(),
                               sample_name=generate_sample_name(),
                               empty_files=False):
    """Create a project directory structure complete with fastq files;
    creates empty files if empty_files is True.
    """
    raise NotImplementedError
    tmp_dir = tempfile.mkdtmp()
    project_dir = os.path.join(tmp_dir, project_name)  #run_dir =
