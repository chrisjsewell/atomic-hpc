import os
import shutil
import pytest
try:
    from unittest import mock
except ImportError:
    import mock
from jsonextended.utils import MockPath
from atomic_hpc.config_yaml import runs_from_config
from atomic_hpc.mockssh import mockserver
from atomic_hpc.deploy_runs import _get_inputs, _replace_in_cmnd, _create_qsub, _deploy_run_normal, _deploy_run_qsub

example_run_local = """runs:
  - id: 1
    name: run_local
    environment: unix
    
    input:
        scripts:
          - input/script.in
        variables:
          var1: value
        files:
          frag1: input/frag.in
    
    process:
        unix:
          run:
            - echo test_echo > output.txt
        qsub:
            walltime: 1:10
            modules:
                - quantum-espresso
                - intel-suite
                - mpi
            run: 
                - mpiexec pw.x -i script2.in > main.qe.scf.out  
            from_temp:
                - .other.out
            
    output:
      remove:
        - frag.in
      rename:
        .txt: .other

  - id: 2
    name: run_local_child
    environment: unix
    requires: 1
    
    input:
        files:
          other: input/other.in

    process:
        unix:
          run:
            - echo test_echo2 > output2.txt
        qsub:
          run:
            - echo test_echo2 > output2.txt
"""

example_run_remote = """runs:
  - id: 1
    name: run_remote
    environment: unix

    input:
        remote:
            hostname: {host}
            username: user
            port: {port}
            password: password
        scripts:
          - input/script.in
        variables:
          var1: value
        files:
          frag1: input/frag.in

    process:
        unix:
          run:
            - echo test_echo > output.txt
        qsub:
            walltime: 1:10
            modules:
                - quantum-espresso
                - intel-suite
                - mpi
            run: 
                - mpiexec pw.x -i script2.in > main.qe.scf.out  
            from_temp:
                - .other.out

    output:
      remote:
         hostname: {host}
         username: user
         port: {port}
         password: password
      remove:
        - frag.in
      rename:
        .txt: .other

  - id: 2
    name: run_remote_child
    environment: unix
    requires: 1
    
    input:
        files:
          other: input/other.in

    process:
        unix:
          run:
            - echo test_echo2 > output2.txt
    
        qsub:
          run:
            - echo test_echo2 > output2.txt
"""

@pytest.fixture("function")
def local_pathlib():

    test_folder = os.path.join(os.path.dirname(__file__), 'test_tmp')
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.mkdir(test_folder)

    configpath = os.path.join(test_folder, "config.yml")
    with open(configpath, "w") as f:
        f.write(example_run_local)
    inpath = os.path.join(test_folder, "input")
    os.mkdir(inpath)
    with open(os.path.join(inpath, 'script.in'), 'w') as f:
            f.write('test @v{var1}\n @f{frag1}')
    with open(os.path.join(inpath, 'frag.in'), 'w') as f:
        f.write('replace\n frag')
    with open(os.path.join(inpath, 'other.in'), 'w') as f:
        f.write('another file')

    yield runs_from_config(configpath), test_folder


@pytest.fixture("function")
def local_mock():
    config_file = MockPath('config.yml', is_file=True,
                        content=example_run_local)
    test_folder = MockPath("test_tmp",
                           structure=[config_file,
                                      {"input": [
                                          MockPath('script.in', is_file=True, content='test @v{var1}\n @f{frag1}'),
                                          MockPath('frag.in', is_file=True, content='replace\n frag'),
                                          MockPath('other.in', is_file=True, content='another file'),
                                      ]}])

    yield runs_from_config(config_file), test_folder


@pytest.fixture("function")
def remote():
    test_folder = os.path.join(os.path.dirname(__file__), 'test_tmp')
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.mkdir(test_folder)
    configpath = os.path.abspath(os.path.join(test_folder, "config.yml"))
    inpath = os.path.join(test_folder, "input")
    os.mkdir(inpath)
    with open(os.path.join(inpath, 'script.in'), 'w') as f:
        f.write('test @v{var1}\n @f{frag1}')
    with open(os.path.join(inpath, 'frag.in'), 'w') as f:
        f.write('replace\n frag')
    with open(os.path.join(inpath, 'other.in'), 'w') as f:
        f.write('another file')

    with mockserver.Server({"user": {"password": "password"}}, test_folder) as server:

        with open(configpath, "w") as f:
            f.write(example_run_remote.format(host=server.host, port=server.port))

        yield runs_from_config(configpath), test_folder


# a better way to do this is in the works: https://docs.pytest.org/en/latest/proposals/parametrize_with_fixtures.html
@pytest.fixture(params=['local_pathlib', 'local_mock', 'remote'])
def context(request):
    return request.getfuncargvalue(request.param)


def test_get_inputs(context):
    top_level, path = context
    variables, files, scripts = _get_inputs(top_level[0], path)
    assert variables == {"var1": "value"}
    assert files == {'frag.in': 'replace\n frag'}
    assert scripts == {"script.in": 'test value\n replace\n frag'}


def test_get_inputs_missing_variable_in_script(context):
    top_level, path = context
    run = top_level[0]
    run["input"]["variables"] = {}
    with pytest.raises(KeyError):
        variables, files, scripts = _get_inputs(run, path)


def test_get_inputs_missing_file_in_script(context):
    top_level, path = context
    run = top_level[0]
    run["input"]["files"] = {}
    with pytest.raises(KeyError):
        variables, files, scripts = _get_inputs(run, path)


def test_get_inputs_missing_file(context):
    top_level, path = context
    run = top_level[0]
    run["input"]["files"] = {"other_file": "other_file.in"}
    with pytest.raises(ValueError):
        variables, files, scripts = _get_inputs(run, path)


def test_replace_in_cmnd():
    cmnd = _replace_in_cmnd("mpirun -np @v{nprocs} script.in -a @v{other} > file.out", {"nprocs": 2, "other": "val"}, 1)
    assert cmnd == "mpirun -np 2 script.in -a val > file.out"

    with pytest.raises(KeyError):
        _replace_in_cmnd("mpirun -np @v{nprocs} script.in > file.out", {}, 1)


def test_run_deploy_normal_samefolder(context):
    top_level, path = context
    _deploy_run_normal(top_level[0], path)

    if not hasattr(path, "to_string"):
        assert (os.path.exists(os.path.join(str(path), 'output/1_run_local/output.other')) or
                os.path.exists(os.path.join(str(path), 'output/1_run_remote/output.other')))
        assert (os.path.exists(os.path.join(str(path), 'output/1_run_local/output2.txt')) or
                os.path.exists(os.path.join(str(path), 'output/1_run_remote/output2.txt')))
    else:
        expected = """Folder("test_tmp")
  File("config.yml") Contents:
   runs:
     - id: 1
       name: run_local
       environment: unix
       
       input:
           scripts:
             - input/script.in
           variables:
             var1: value
           files:
             frag1: input/frag.in
       
       process:
           unix:
             run:
               - echo test_echo > output.txt
           qsub:
               walltime: 1:10
               modules:
                   - quantum-espresso
                   - intel-suite
                   - mpi
               run: 
                   - mpiexec pw.x -i script2.in > main.qe.scf.out  
               from_temp:
                   - .other.out
               
       output:
         remove:
           - frag.in
         rename:
           .txt: .other
   
     - id: 2
       name: run_local_child
       environment: unix
       requires: 1
       
       input:
           files:
             other: input/other.in
   
       process:
           unix:
             run:
               - echo test_echo2 > output2.txt
           qsub:
             run:
               - echo test_echo2 > output2.txt
  Folder("input")
    File("frag.in") Contents:
     replace
      frag
    File("other.in") Contents:
     another file
    File("script.in") Contents:
     test @v{var1}
      @f{frag1}
  Folder("output")
    Folder("1_run_local")
      File("other.in") Contents:
       another file
      File("output.other") Contents:
       test_echo
      File("output2.txt") Contents:
       test_echo2
      File("script.in") Contents:
       test value
        replace
        frag"""

        assert path.to_string(file_content=True) == expected


def test_run_deploy_normal_separate_folder(context):
    top_level, path = context
    _deploy_run_normal(top_level[0], path, separate_dir=True)

    if not hasattr(path, "to_string"):
        assert (os.path.exists(os.path.join(str(path), 'output/1_run_local/output.other')) or
                os.path.exists(os.path.join(str(path), 'output/1_run_remote/output.other')))
        assert (os.path.exists(os.path.join(str(path), 'output/2_run_local_child/output2.txt')) or
                os.path.exists(os.path.join(str(path), 'output/2_run_remote_child/output2.txt')))
    else:
        expected = """Folder("test_tmp")
  File("config.yml") Contents:
   runs:
     - id: 1
       name: run_local
       environment: unix
       
       input:
           scripts:
             - input/script.in
           variables:
             var1: value
           files:
             frag1: input/frag.in
       
       process:
           unix:
             run:
               - echo test_echo > output.txt
           qsub:
               walltime: 1:10
               modules:
                   - quantum-espresso
                   - intel-suite
                   - mpi
               run: 
                   - mpiexec pw.x -i script2.in > main.qe.scf.out  
               from_temp:
                   - .other.out
               
       output:
         remove:
           - frag.in
         rename:
           .txt: .other
   
     - id: 2
       name: run_local_child
       environment: unix
       requires: 1
       
       input:
           files:
             other: input/other.in
   
       process:
           unix:
             run:
               - echo test_echo2 > output2.txt
           qsub:
             run:
               - echo test_echo2 > output2.txt
  Folder("input")
    File("frag.in") Contents:
     replace
      frag
    File("other.in") Contents:
     another file
    File("script.in") Contents:
     test @v{var1}
      @f{frag1}
  Folder("output")
    Folder("1_run_local")
      File("output.other") Contents:
       test_echo
      File("script.in") Contents:
       test value
        replace
        frag
    Folder("2_run_local_child")
      File("other.in") Contents:
       another file
      File("output.other") Contents:
       test_echo
      File("output2.txt") Contents:
       test_echo2
      File("script.in") Contents:
       test value
        replace
        frag"""

        assert path.to_string(file_content=True) == expected


def test_create_qsub(context):
    top_level, path = context
    qsub = top_level[0]["process"]["qsub"]
    qsub["jobname"] = "1_test"

    out = _create_qsub(qsub, "path/to/dir",
                       {(1, "test_run"): {"modules": qsub["modules"],
                            "fnames": ["test.txt", "other.txt"],
                            "copyregex": ["*"],
                            "execruns": ["mpiexec something"]}})

    # assert "#PBS -N 1_test" in out
    # assert "#PBS -l walltime=1:10:00" in out
    expected = """#!/bin/bash --login
#PBS -N 1_test
#PBS -l walltime=1:10:00
#PBS -l select=1:ncpus=16
#PBS -j oe


echo "<qstat -f $PBS_JOBID>"
qstat -f $PBS_JOBID
echo "</qstat -f $PBS_JOBID>"

# number of cores per node used
export NCORES=16
# number of processes
export NPROCESSES=16

# Make sure any symbolic links are resolved to absolute path
export PBS_O_WORKDIR=$(readlink -f $PBS_O_WORKDIR)

# Set the number of threads to 1
#   This prevents any system libraries from automatically 
#   using threading.
export OMP_NUM_THREADS=1

1 - test_run
============
echo Running: 1 - test_run
============

# load required modules
module load quantum-espresso intel-suite mpi

# copy required input files from $WORKDIR to $TMPDIR
cp -p path/to/dir/test.txt $TMPDIR
cp -p path/to/dir/other.txt $TMPDIR

# main commands to run (in $TMPDIR)
mpiexec something

# copy required output files from $TMPDIR to $WORKDIR
cp -pR $TMPDIR/* path/to/dir 


"""
    assert out == expected


def test_run_deploy_qsub_fail(context):
    top_level, path = context
    run = top_level[0]
    run["environment"] = "qsub"

    #with pytest.raises(RuntimeError):
    assert _deploy_run_qsub(top_level[0], path, exec_errors=True) == False


def test_run_deploy_qsub_pass(context):
    top_level, path = context
    run = top_level[0]
    run["environment"] = "qsub"

    with mock.patch("atomic_hpc.context_folder.LocalPath.exec_cmnd", return_value=None):
        with mock.patch("atomic_hpc.context_folder.RemotePath.exec_cmnd", return_value=None):
            _deploy_run_qsub(top_level[0], path)

    if hasattr(path, "to_string"):
        expected_struct = """Folder("test_tmp")
  File("config.yml")
  Folder("input")
    File("frag.in")
    File("other.in")
    File("script.in")
  Folder("output")
    Folder("1_run_local")
      File("frag.in")
      File("other.in")
      File("run.qsub")
      File("script.in")"""
        assert path.to_string() == expected_struct

        expected_qsub = """File("run.qsub") Contents:
#!/bin/bash --login
#PBS -N 1_run_local
#PBS -l walltime=1:10:00
#PBS -l select=1:ncpus=16
#PBS -j oe


echo "<qstat -f $PBS_JOBID>"
qstat -f $PBS_JOBID
echo "</qstat -f $PBS_JOBID>"

# number of cores per node used
export NCORES=16
# number of processes
export NPROCESSES=16

# Make sure any symbolic links are resolved to absolute path
export PBS_O_WORKDIR=$(readlink -f $PBS_O_WORKDIR)

# Set the number of threads to 1
#   This prevents any system libraries from automatically 
#   using threading.
export OMP_NUM_THREADS=1

1 - run_local
=============
echo Running: 1 - run_local
=============

# load required modules
module load quantum-espresso intel-suite mpi

# copy required input files from $WORKDIR to $TMPDIR
cp -p test_tmp/output/1_run_local/script.in $TMPDIR
cp -p test_tmp/output/1_run_local/frag.in $TMPDIR

# main commands to run (in $TMPDIR)
mpiexec pw.x -i script2.in > main.qe.scf.out

# copy required output files from $TMPDIR to $WORKDIR
cp -pR $TMPDIR/*.other.out test_tmp/output/1_run_local 


2 - run_local_child
===================
echo Running: 2 - run_local_child
===================

# load required modules


# copy required input files from $WORKDIR to $TMPDIR
cp -p test_tmp/output/1_run_local/other.in $TMPDIR

# main commands to run (in $TMPDIR)
echo test_echo2 > output2.txt

# copy required output files from $TMPDIR to $WORKDIR

"""

        assert path["output/1_run_local/run.qsub"].to_string(file_content=True) == expected_qsub

