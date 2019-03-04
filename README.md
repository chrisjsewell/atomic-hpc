atomic-hpc
==========

[![travis](https://travis-ci.org/chrisjsewell/atomic-hpc.svg?branch=master)](https://travis-ci.org/chrisjsewell/atomic-hpc)
[![coveralls](https://coveralls.io/repos/github/chrisjsewell/atomic-hpc/badge.svg?branch=master)](https://coveralls.io/github/chrisjsewell/atomic-hpc?branch=master)
[![PyPI](https://img.shields.io/pypi/v/atomic-hpc.svg)](https://pypi.python.org/pypi/atomic-hpc/)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/atomic-hpc/badges/version.svg)](https://anaconda.org/conda-forge/atomic-hpc)

<!-- [![codacy](https://api.codacy.com/project/badge/Grade/e0b541be3f834f12b77c712433ee64c9)](https://www.codacy.com/app/chrisj_sewell/atomic-hpc?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=chrisjsewell/atomic-hpc&amp;utm_campaign=Badge_Grade) -->

**Project**: <https://github.com/chrisjsewell/atomic-hpc>

A package for running multiple executable scripts on both local and remote hosts,
configured using a modern standard [YAML](https://en.wikipedia.org/wiki/YAML) file.
This package was designed, in particular, for job submission to High Performance Computing (HPC) clusters, such as the
[Imperial HPC facility](https://www.imperial.ac.uk/admin-services/ict/self-service/research-support/hpc/).
Working examples can be found [here](https://github.com/chrisjsewell/atomic-hpc/tree/master/examples).

Installation
------------

It is recommended to setup an [Anaconda](https://docs.continuum.io/anaconda/install/) environment.

    >> conda install -c conda-forge atomic-hpc

Optionally install atomic-hpc using pip:

    >> pip install atomic-hpc

To use conda on the Imperial HPC (not required), run the following (as outlined on the [wiki](https://wiki.imperial.ac.uk/display/HPC/Python)):

    >> module load anaconda3/personal
    >> anaconda-setup

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

**config_remote.yaml**

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
      qsub:
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

To retrieve outputs from a remote host, once all processes have run:

    >> retrieve_config config_remote.yaml -o path/to/local/outputs

Inputs
------

Input files and scripts can be local or remote and will be copied to the output folder before the runs.
Variables can also be set that will be replaced in the cmnd lines and script files if a corresponding `@v{var_id}`
regex is found. Similarly entire file contents can be parsed to the script with the `@f{file_id}` regex:

```
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

```console
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

Note1: all relative paths are resolved relative to the execution directory, unless set with `run_config -b base/path/`.

Note2: For the above example, if a script or cmndline has `@v{file1}` in it (rather than `@f{file1}`), 
then this would be replaced with the file name (rather than its content), i.e. input.txt

Note3: Within `qsub: run:`, the keyword `@{wrkpath}` will be replaced with the working folder path.
This is handy, for instance, to maintain a dynamic log file in the work path, while the program is running in a temporary folder, e,g,

```yaml
process:
    qsub:
      start_in_temp: true
      run:
        - my_program > @{wrkpath}/output.log
```

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
    binaries:
      file2: path/to/file2
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
      # can also use wildcard characters *, ? and []
      - tmp/
    rename: 
      # renames any segment of file names, i.e. file.other.out.txt -> file.other.qe.txt
      # searches for files (recursively) in all folders
      .other.out: .other.qe
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
      memory_per_node: 1gb
      tmpspace: 500gb # minimum free space required on the temporary directory
      walltime: 1:00:00
      queue: queue_name
      email: bob@hotmail.com # send email on job start/end
      # NB: the emailling feature has recently been disabled on the Imperial HPC
      modules:
        - module1
        - module2
      start_in_temp: true # if true cd to $TMPDIR and copy all files before running executables
      run:
        - mpiexec pw.x -i script2.in > main.qe.scf.out
  id: 1
  name: run1
```

Setting up an SSH Public and Private Keys
-----------------------------------------

Rather than directly using a password to access the remote host, it is reccommended that a public key authentication 
be used, as a more secure authentication method. There are numerous explanations on the internet 
(including [here](https://help.ubuntu.com/community/SSH/OpenSSH/Keys)) and below follows a short setup guide 
(taken from [here](https://wiki.ch.ic.ac.uk/wiki/index.php?title=Mod:Hunt_Research_Group/SSHkeyfile)):

First open a shell on the computer you want to connect from. Enter cd ~/.ssh. 
If an `ls` shows to files called 'id_rsa' and 'id_rsa.pub' you already have a key pair. 
If not, enter `ssh-keygen` Here is what the result should look like:

```
heiko@clove:~/.ssh$ ssh-keygen 
Generating public/private rsa key pair.
Enter file in which to save the key (/Users/heiko/.ssh/id_rsa):
Enter passphrase (empty for no passphrase): 
Enter same passphrase again: 
Your identification has been saved in id_rsa.
Your public key has been saved in id_rsa.pub.
The key fingerprint is:
f0:da:dc:77:cf:71:12:c8:50:dc:18:a9:8d:66:38:ae heiko@clove.ch.ic.ac.uk
The key's randomart image is:
+--[ RSA 2048]----+
|           .o=   |
|           .+ .  |
|      .  ..+     |
|       oo =o..   |
|       .S+  o .  |
|       +..     . |
|      ..o . . o..|
|      E    . . +o|
|                o|
+-----------------+
```

You should keep the standard directory and choose a suitably difficult passphrase.

The two file you just created are key and keyhole. The first file 'id_rsa' is the key. 
You should not ever ever ever give it to anybody else or allow anyone to copy it. 
The second file 'id_rsa.pub' the keyhole. It is public and you could give it to anyone. 
In this case, give it to the hpc.

If you open 'id_rsa.pub' it should contain one line of, similar to:

    ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAwRDgM+iQg7OaX/CFq1sZ9jl206nYIhW9SMBqsOIRvGM68/6o6uxZo/D4IlmQI9sAcU5FVNEt9dvDanRqUlC7ZtcOGOCqZsj1HTGD3LcOiPNHYPvi1auEwrXv1hDh4pmJwdgZCRnpewNl+I6RNBiZUyzLzp0/2eIyf4TqG1rpHRNjmtS9turANIv1GK1ONIO7RfVmmIk/jjTQJU9iJqje9ZSXTSm7rUG4W8q+mWcnACReVChc+9mVZDOb3gUZV1Vs8e7G36nj6XfHw51y1B1lrlnPQJ7U3JdqPz6AG3Je39cR1vnfALxBSpF5QbTHTJOX5ke+sNKo//kDyWWlfzz3rQ== heiko@clove.ch.ic.ac.uk

Now log in to the HPC and open (or create) the file '~/.ssh/authorized_keys'. 
In a new line at the end of this file, you should add a comment (starting with #) about where that keypair comes from
and then in a second line you should copy and paste the complete contents of your 'id_rsa.pub' file.

    #MAC in the office
    ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAwRDgM+iQg7OaX/CFq1sZ9jl206nYIhW9SMBqsOIRvGM68/6o6uxZo/D4IlmQI9sAcU5FVNEt9dvDanRqUlC7ZtcOGOCqZsj1HTGD3LcOiPNHYPvi1auEwrXv1hDh4pmJwdgZCRnpewNl+I6RNBiZUyzLzp0/2eIyf4TqG1rpHRNjmtS9turANIv1GK1ONIO7RfVmmIk/jjTQJU9iJqje9ZSXTSm7rUG4W8q+mWcnACReVChc+9mVZDOb3gUZV1Vs8e7G36nj6XfHw51y1B1lrlnPQJ7U3JdqPz6AG3Je39cR1vnfALxBSpF5QbTHTJOX5ke+sNKo//kDyWWlfzz3rQ== heiko@clove.ch.ic.ac.uk

Close the 'authorized_keys' file and your connection to the HPC. Now connect again. 
You will be asked for the passphrase for your keyfile. Enter it. 
You should now be logged in to the HPC. If you are not asked for the passphrase but for the password of your account, 
the Server does not accept your key pair. 

So far, we have replaced entering the password for your account with entering the passphrase for your keypair. 
This is where a so called SSH-agent comes handy. The agent will store your passphrases for you so you do not have 
to enter them anymore. Luckily MacOS has one build in, that should have popped up and asked you, whether you want the 
agent to take care of your passphrases. If you said 'YES', that was the very last time you ever heard or saw anything of 
it or your passphrase. Similar agents exist for more or less every OS. From now on you just have to 
enter hostname and username and you are logged in.

Notes
-----

If using special characters in strings (like \*) be sure to wrap them in "" or use the > or | yaml components 
(see https://en.wikipedia.org/wiki/YAML#Basic_components)
