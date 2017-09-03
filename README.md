atomic-hpc
==========

[![image](https://travis-ci.org/chrisjsewell/jsonextended.svg?branch=master)](https://travis-ci.org/chrisjsewell/atomic-hpc)
[![image](https://coveralls.io/repos/github/chrisjsewell/jsonextended/badge.svg?branch=master)](https://coveralls.io/github/chrisjsewell/atomic-hpc?branch=master)
[![image](https://api.codacy.com/project/badge/Grade/e0b541be3f834f12b77c712433ee64c9)](https://www.codacy.com/app/chrisj_sewell/atomic-hpc?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=chrisjsewell/atomic-hpc&amp;utm_campaign=Badge_Grade)

**Project**: <https://github.com/chrisjsewell/atomic-hpc>

A package for running multiple executable scripts on both local and remote hosts, 
configured using a modern standard [YAML](https://en.wikipedia.org/wiki/YAML) file. 
This package was designed, in particular, for job submission to High Performance Computing (HPC) clusters, such as the
[Imperial HPC facility](https://www.imperial.ac.uk/admin-services/ict/self-service/research-support/hpc/).

Installation
------------

It is recommended to setup an [Anaconda](https://docs.continuum.io/anaconda/install/) environment. 
For the Imperial HPC, run the following (as outlined on the [wiki](https://wiki.imperial.ac.uk/display/HPC/Python)):

    >> module load anaconda3/personal
    >> anaconda-setup
    
Then install atomic-hpc simply by:

    >> pip install atomic-hpc

Minimal Example
---------------

1. Write a yaml configuration file

config.yaml:
```yaml
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
```

2. Submit it with the command line app:

        >> run_config config.yaml
    
3. The results will be available in the output path:

        >> ls -R output
        output/1_test_local:
        config_1.yaml     hallo.txt
    

Notes
-----

If using special characters in strings (like \*) be sure to wrap them in "" or use the > or | yaml components 
(see https://en.wikipedia.org/wiki/YAML#Basic_components)
