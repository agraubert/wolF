from setuptools import setup
import re
import os
import sys

ver_info = sys.version_info
if ver_info < (3,5,4):
    raise RuntimeError("wolf requires at least python 3.5.4")

#with open(os.path.join(os.path.dirname(__file__), 'canine', 'orchestrator.py')) as r:
#    version = re.search(r'version = \'(\d+\.\d+\.\d+[-_a-zA-Z0-9]*)\'', r.read()).group(1)

#with open("README.md") as r:
#    long_description = r.read()

setup(
    name = 'wolf_flow',
    version = "0.0.1",
    packages = [
        'wolf'
    ],
    description = 'Interactively create computational workflows in Python',
    url = 'https://github.com/getzlab/wolf',
    author = 'Aaron Graubert/Julian Hess - Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'jhess@broadinstitute.org',
    #long_description = long_description,
    #long_description_content_type = 'text/markdown',
    install_requires = [
        'canine>=0.7.0',
        'pandas>=0.24.1'
    ],
    classifiers = [
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: System :: Clustering",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: BSD License"
    ],
    license="BSD3"
)
