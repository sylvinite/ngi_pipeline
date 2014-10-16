""" Small Flask server to trigger bioinforamatics analysis.
"""
from flask import Flask, request

from ngi_pipeline.conductor import flowcell


app = Flask('NGI Pipeline Server')

@app.route('/ngi_pipeline/process_demultiplexed_flowcell/<flowcell_id>', methods=['GET'])
def process_demultiplexed_flowcell(flowcell_id):
    """ Sort demultiplexed Illumina flowcells into projects and launch their analysis.
    """
    assert request.method == 'GET'
    # XXX: Check that the flowcell_id matches some regular expression
    restrict_to_projects = request.args.get('restrict_to_projects', None)
    restrict_to_samples = request.args.get('restrict_to_samples', None)
    restart_failed_jobs = request.args.get('restart_failed_jobs', False)
    config_file_path = request.args.get('config_file_path', None)
    #flowcell.process_demultiplexed_flowcell(flowcell_id, restrict_to_projects,
    #        restrict_to_samples, restart_failed_jobs, config_file_path)
    # XXX: Return correct response codes, check errors, MAKE CALL ASYNCHRONOUS, etc...
    return "Flowcell {} started being processed".format(flowcell_id)


if __name__ == '__main__':
    app.run()
