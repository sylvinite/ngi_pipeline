"""Here we will keep results parsers for the various output files produced by Piper."""
import sys

from collections import namedtuple
from ngi_pipeline.log.loggers import minimal_logger


LOG = minimal_logger(__name__)

## Maybe define parameters
def parse_results_for_workflow(workflow_name, *args, **kwargs):
    parser_fn_name = "parse_{}".format(workflow_name)
    try:
        parser_function = getattr(sys.modules[__name__], parser_fn_name)
    except AttributeError as e:
        error_msg = 'Workflow "{}" has no associated parser implemented.'.format(workflow_name)
        LOG.error(error_msg)
        raise NotImplementedError(error_msg)
    return parser_function(*args, **kwargs)


def parse_qualimap_coverage(genome_results_file):
    autosomal_cov_length = 0
    autosomal_cov_bases = 0
    coverage_section = False
    with open(genome_results_file, 'r') as f:
        for line in f:
            if line.startswith('>>>>>>> Coverage per contig'):
                coverage_section = True
                continue
            if coverage_section:
                line = line.strip()
                if line:
                    sections = line.split()
                    if sections[0].isdigit() and int(sections[0]) <= 22:
                        autosomal_cov_length += float(sections[1])
                        autosomal_cov_bases += float(sections[2])
        if autosomal_cov_length and autosomal_cov_bases:
            return autosomal_cov_bases / autosomal_cov_length
        else:
            return 0.0

def parse_genotype_concordance_file(genotype_concordance_file):
    concordance_data = []
    gt_values_list = []
    with open(genotype_concordance_file, 'r') as f:
        for line in iter(f.readline, ''):
            header_location = None
            if line.startswith("#:GATKTable:GenotypeConcordance_Summary"):
                header_values = [h.strip() for h in f.readline().strip().split('  ') if h.strip()]
                header_values = [h.lower().replace("-", "_").replace(" ", "_") for h in header_values]
                GTValue = namedtuple('GTValue', header_values)
                f.readline() # Skip first ("ALL") summary line
                data_location = f.tell()
                break
        else:
            raise ValueError('Unable to find genotype concordance summary '
                             'section in genotype file "{}"'.format(genotype_concordance_file))
    with open(genotype_concordance_file, 'r') as f:
        f.seek(data_location)
        for line in iter(f.readline, ''):
            if line.strip() != "":
                try:
                    gt_values_list.append(GTValue._make(line.strip().split()))
                except TypeError as e:
                    LOG.error('Unable to parse genotype concordance line "{}"; number '
                              'of data fields does not match number of header fields '
                              'fields ({}); skipping'.format(" ".join(line.strip().split()), e))
                    continue
            else:
                break
    samples_gtc_dict = {}
    for gt_entry in gt_values_list:
        try:
            samples_gtc_dict[gt_entry.sample] = float(gt_entry.overall_genotype_concordance)
        except ValueError as e:
            LOG.error('Unable to parse overall genotype concordance '
                      'value for sample "{}" (value "{}" is not a '
                      'number)'.format(gt_entry.sample,
                                       gt_entry.overall_genotype_concordance))
            continue
    return samples_gtc_dict
