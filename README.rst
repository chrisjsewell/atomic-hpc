atomic-hpc
==========

|travis| |coverall| |codacy|

**Project**: https://github.com/chrisjsewell/atomic-hpc

A package for running multiple executable scripts on both local and
remote hosts, configured using a modern standard
`YAML <https://en.wikipedia.org/wiki/YAML>`__ file. This package was
designed, in particular, for job submission to High Performance
Computing (HPC) clusters, such as the `Imperial HPC
facility <https://www.imperial.ac.uk/admin-services/ict/self-service/research-support/hpc/>`__.

Installation
------------

It is recommended to setup an
`Anaconda <https://docs.continuum.io/anaconda/install/>`__ environment.
For the Imperial HPC, run the following (as outlined on the
`wiki <https://wiki.imperial.ac.uk/display/HPC/Python>`__):

::

    >> module load anaconda3/personal
    >> anaconda-setup

Then install atomic-hpc simply by:

::

    >> pip install atomic-hpc

Minimal Example
---------------

1. Write a yaml configuration file

config.yaml:

.. code:: yaml

    runs:
      - id: 1
        name: test_local
        environment: unix

        process:
          unix:
            run:
              - echo "hallo world" > hallo.txt
        
        output:
          path: output

2. Submit it with the command line app:

   ::

       >> run_config config.yaml

3. The results will be available in the output path:

   ::

       >> ls -R output
       output/1_test_local:
       config_1.yaml     hallo.txt

Notes
-----

If using special characters in strings (like \*) be sure to wrap them in
"" or use the > or \| yaml components (see
https://en.wikipedia.org/wiki/YAML#Basic\_components)

.. |travis| image:: https://travis-ci.org/chrisjsewell/jsonextended.svg?branch=master
   :target: https://travis-ci.org/chrisjsewell/atomic-hpc
.. |coverall| image:: https://coveralls.io/repos/github/chrisjsewell/jsonextended/badge.svg?branch=master
   :target: https://coveralls.io/github/chrisjsewell/atomic-hpc?branch=master
.. |codacy| image:: https://api.codacy.com/project/badge/Grade/e0b541be3f834f12b77c712433ee64c9
   :target: https://www.codacy.com/app/chrisj_sewell/atomic-hpc?utm_source=github.com&utm_medium=referral&utm_content=chrisjsewell/atomic-hpc&utm_campaign=Badge_Grade
