#!/usr/bin/env python

import os
import pip
from setuptools import setup, find_packages


# Parse requirements:
req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
requires = list(pip.req.parse_requirements(req_path, session=pip.download.PipSession()))

setup(
    name='yan',
    version='0.1.0',
    description='Yandex News feed parser',
    author='Sergei Fomin',
    author_email='sergio-dna@yandex.ru',
    install_requires=[str(r.req) for r in requires if r.req],
    dependency_links=[str(r.link) for r in requires if r.link],
    packages=find_packages()
)
