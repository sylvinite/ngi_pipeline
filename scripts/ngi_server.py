""" Small Tornado server to trigger bioinforamatics analysis.
"""
import tornado.ioloop
import tornado.web

from ngi_pipeline.conductor import flowcell

class ProcessDemultiplexedFlowcellHandler(tornado.web.RequestHandler):
    def get(self, flowcell_id):
        restrict_to_projects = self.get_argument('restrict_to_projects', None)
        restrict_to_samples = self.get_argument('restrict_to_samples', None)
        restart_failed_jobs = self.get_argument('restart_failed_jobs', False)
        config_file_path = self.get_argument('config_file_path', None)
        #flowcell.process_demultiplexed_flowcell(flowcell_id, restrict_to_projects,
        #        restrict_to_samples, restart_failed_jobs, config_file_path)
        self.write("Flowcell {} started being processed".format(flowcell_id))

application = tornado.web.Application([
    # Here a regex that really matches a flowcell
    (r"/ngi_pipeline/process_demultiplexed_flowcell/([^/]*)$", ProcessDemultiplexedFlowcellHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()

