"""Here we will keep results parsers for the various output files produced by Piper.
I suppose these will probably return dictionaries."""

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
    with open(genome_results_file, 'r') as f:
        for line in f:
            if line.startswith('>>>>>>> Coverage per contig'):
                coverage_section = True
                continue
            line = line.strip()
            if coverage_section and line:
                sections = line.split()
                if sections[0].isdigit() and int(sections[0]) <= 22:
                    autosomal_cov_length += float(sections[1])
                    autosomal_cov_bases += float(sections[2])
        if autosomal_cov_length and autosomal_cov_bases:
            return autosomal_cov_bases / autosomal_cov_length
        else:
            return 0.0
