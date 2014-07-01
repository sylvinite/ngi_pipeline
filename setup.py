#!/usr/bin/env python

import glob
import os
import subprocess
import sys

from setuptools import setup, find_packages

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
      install_requires=install_requires,
      packages=find_packages()
      )
