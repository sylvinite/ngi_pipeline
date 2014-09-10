Software designed to take demultiplexed Illumina flowcells and shove 'em through an analysis pipeline (e.g. Piper/GATK).

=== Installation ===
- clone this repo some place of your choosing, let's say $HOME
- install conda (http://repo.continuum.io/miniconda/Miniconda-3.6.0-Linux-x86_64.sh)
- conda create -n ngi pip sqlalchemy
- mkdir $HOME/.ngipipeline && cp $HOME/ngi_pipeline/nestor_ngi_config.yaml $HOME/.ngipipeline/
- get your API token from userman and export them sheez in your .bashrc or somewhere like
  - (include example here)
- 
