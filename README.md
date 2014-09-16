Software designed to take demultiplexed Illumina flowcells and shove 'em through an analysis pipeline (e.g. Piper/GATK).

Installation
============

Clone this repo some place of your choosing, let's say $HOME:

`cd && git clone https://github.com/NationalGenomicsInfrastructure/ngi_pipeline.git`

Install conda (http://repo.continuum.io/miniconda/Miniconda-3.6.0-Linux-x86_64.sh):

`wget http://repo.continuum.io/miniconda/Miniconda-3.6.0-Linux-x86_64.sh && bash Miniconda-3.6.0-Linux-x86_64.sh`

Create a new environment for NGI (installing some prereqs at the same time):

`conda create -n ngi pip sqlalchemy`

Activate this environment and install ngi_pipeline:

`source activate ngi && python $HOME/ngi_pipeline/setup.py install`

Copy the relevant configuration file (milou or nestor) to the default location:

`mkdir $HOME/.ngipipeline && cp $HOME/ngi_pipeline/milou_ngi_config.yaml $HOME/.ngipipeline/`

Get your API token from Charon (on the charon-dev website click "Users" and then click your username) and export this along with the base URL via your `.bashrc`:

```
export CHARON_API_TOKEN=<your token here>
export CHARON_BASE_URL='http://charon-dev.scilifelab.se'
```

Set up your Piper paths appropriately in your `.bashrc`:

```
# PIPER
module load java/sun_jdk1.7.0_25
PIPER_BASE_PATH=/proj/a2010002/nobackup/NGI
export PIPER_GLOB_CONF_XML=$PIPER_BASE_PATH/Bin/Piper/workflows/uppmax_global_config.xml
export PATH=$PATH:$PIPER_BASE_PATH/Bin/Piper/bin
export PATH=$PATH:$PIPER_BASE_PATH/Bin/Piper/workflows/
export LD_LIBRARY_PATH=/sw/apps/build/slurm-drmaa/default/lib/:$LD_LIBRARY_PATH
```

Are there more steps? Probably. Email me when you find out what they are.
