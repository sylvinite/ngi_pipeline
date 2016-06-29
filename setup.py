#!/usr/bin/env python
import glob
from ngi_pipeline import __version__
from setuptools import setup, find_packages

version = __version__

try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

setup(name="ngi_pipeline",
      author = "Science for Life Laboratory",
      author_email = "mario@scilifelab.se",
      description = "Infrastructure/analysis pipeline scripts.",
      license = "MIT",
      url="https://github.com/scilifelab/scilifelab_pipeline",
      version=version,
      install_requires=install_requires,
      scripts = glob.glob('scripts/*py'),
      packages=find_packages()
      )
