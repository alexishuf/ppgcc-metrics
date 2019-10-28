# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='ppgcc_metrics',
    version='0.1.0',
    description='Gather input datasets for brazillian graduate progrmam (self-)evaluation',
    long_description=readme,
    author='Alexis Huf',
    author_email='alexishuf@gmail.com',
    url='https://github.com/alexishuf/ppgcc-metrics',
    license=license,
    packages=find_packages(exclude=('data', 'tests'))
)
