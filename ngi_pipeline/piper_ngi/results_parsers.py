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
