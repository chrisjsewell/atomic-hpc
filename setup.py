#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Setup for atomic-hpc."""

import io
from importlib import import_module
from setuptools import setup  # , find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
with open('test_requirements.txt') as f:
    test_requirements = f.read().splitlines()

with io.open('README.md') as readme:
    setup(
        name='atomic-hpc',
        version=import_module('atomic_hpc').__version__,
        description=(
            'A package for running multiple executable scripts on both '
            'local and remote hosts, configured using a YAML file'),
        long_description=readme.read(),
        long_description_content_type='text/markdown',
        install_requires=requirements,
        tests_require=test_requirements,
        license='MIT',
        author='Chris Sewell',
        author_email='chrisj_sewell@hotmail.com',
        url='https://github.com/chrisjsewell/atomic-hpc',
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: End Users/Desktop',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Topic :: Scientific/Engineering :: Chemistry',
            'Topic :: Scientific/Engineering :: Physics',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'Topic :: Utilities',
        ],
        keywords='yaml, hpc, configuration, ssh, sftp',
        zip_safe=True,
        package_data={
            "mockssh": [
                "sample-user-key",
                "sample-user-key.pub",
                "server-key",
                "server-key.pub",
                "wrong-user-key"
            ],
        },
        scripts=['bin/run_config', "bin/retrieve_config"],
    )
