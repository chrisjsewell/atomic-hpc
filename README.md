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

1. Write a yaml configuration file; each run must have a name and a unique id, 
then attributes can be set in the (global) `defaults` section or per run (run attributes will overwrite defaults):

    **config.yaml**:
    ```yaml
    defaults:
        environment: unix
    
        process:
          unix:
            run:
              - echo "hallo world" > hallo.txt
        
        output:
          path: output
    runs:
      - id: 1
        name: test_local
      - id: 2
        name: test_other
    
    ```

2. Submit it with the command line app (use -h to see all options):

        >> run_config config.yaml
    
3. The results will be available in the output path, with one folder per run:

        >> ls -R output
        output/1_test_local:
        config_1.yaml     hallo.txt
        output/2_test_other:
        config_2.yaml     hallo.txt
    

Minimal Example (Remote and PBS)
--------------------------------

Jobs can be submitted to remote hosts and, optionally, 
[PBS](https://en.wikipedia.org/wiki/Portable_Batch_System) type systems.

```yaml
runs:
  - id: 1
    name: test_qsub
    environment: qsub

    output:
      remote:
        hostname: login.cx1.hpc.imperial.ac.uk
        username: cjs14
      path: /work/cjs14/yaml_test

    process:
        cores_per_node: 16  
        nnodes: 1     
        walltime: 1:00:00
        queue: queue_name
        modules:
            - quantum-espresso
            - intel-suite
            - mpi
        run: 
            - mpiexec pw.x -i script2.in > main.qe.scf.out  
```

Inputs
------

Input files and scripts can be local or remote and will be copied to the output folder before the runs.
Variables can also be set that will be replaced in the cmnd lines and script files if a corresponding `@v{var_id}`
regex is found. Similarly entire file contents can be parsed to the script with the `@f{file_id}` regex:

```sh
>> cat path/to/script1.in
@v{var1}
@f{file1}
>> cat path/to/file1
This is file 1
```

 **config.yaml**:
```yaml
defaults:
    description: quantum-espresso run
    environment: unix

    input:
        remote:
            hostname: login.cx1.hpc.imperial.ac.uk
            username: cjs14
        variables:
            var1:
            nprocs: 2
        files:
            file1: path/to/input.txt
        scripts:
        - path/to/script1.in
    
    process:
        unix:
            run:
                - mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out

runs:
  - id: 1
    name: run1
    input:
        variables:
            var1: value1
  - id: 2
    name: run2
    input:
        variables:
            var1: value2

```

**Run**:
```sh
>> run_config config.yaml
>> ls -R output
output/1_run1:
config_1.yaml  input.txt  main.qe.scf.out  script.in
output/2_run2:
config_2.yaml  input.txt  main.qe.scf.out  script.in
>> cat output/1_run1/script.in
value1
This is file 1
```

NB: all relative paths are resolved relative to the execution directory, unless set with `run_config -b base/path/`.

Outputs
-------

As well as specifying the output path, post-process file removal and renaming can be configured:

```yaml
runs:
  - id: 1
    name: run1
    output:
        path: path/to/output
        remove:
            - tmp/
        rename:
            .other.out: .other.qe.json
```

Full Configuration Options
--------------------------

```yaml
runs:
  description: quantum-espresso run
  environment: qsub
  input:
    path:
    scripts:
      - path/to/script1.in
      - path/to/script2.in
    files:
      file1: path/to/file1
    variables:
      var1: overridevalue
      var2: value
      nprocs: 2
    remote:
      hostname: login.cx1.hpc.imperial.ac.uk
      port: 22
      username: cjs14
      password:
      pkey:
      key_filename:
      timeout:
  output:
    remote:
      hostname: login.cx1.hpc.imperial.ac.uk
      port: 22
      username: cjs14
      password:
      pkey:
      key_filename:
      timeout:
    path: path/to/top/level/output
    remove:
      - tmp/
    rename:
      .other.out: .other.qe.out
  process:
    unix:
      run:
        - mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out
    windows:
      run:
        - mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out
    qsub:
      jobname:
      cores_per_node: 16
      nnodes: 1
      walltime: 1:00:00
      queue: queue_name
      email: bob@hotmail.com
      modules:
        - module1
        - module2
      run:
        - mpiexec pw.x -i script2.in > main.qe.scf.out
  id: 1
  name: run1
```

Notes
-----

If using special characters in strings (like \*) be sure to wrap them in "" or use the > or | yaml components 
(see https://en.wikipedia.org/wiki/YAML#Basic_components)
