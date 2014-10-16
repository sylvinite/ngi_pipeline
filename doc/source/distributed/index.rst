.. Distributed mode of the pipeline

Pipeline Distributed Mode
=========================

In order to be able to do the pre-processing (BCL conversion & demultiplexing) locally
and the heavy analysis in our HPC, we've implemented a "distributed" mode.

This mode consists on a Tornado server that runs on our HPC and listens for HTTP
requests that will trigger different analysis. Below follows the technical documentation
of this server script and the definition of the Tornado handlers.

.. XXX Link to the documentation for the ngi_server.py script

.. XXX Link to the autodoc for the distributed.tornado handlers
