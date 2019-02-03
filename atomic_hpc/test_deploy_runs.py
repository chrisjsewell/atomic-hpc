import logging
import os
import shutil
from tempfile import mkdtemp

import pytest
try:
    from unittest import mock
except ImportError:
    import mock
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

from jsonextended.utils import MockPath
from atomic_hpc.config_yaml import format_config_yaml
from atomic_hpc.mockssh import mockserver
from atomic_hpc.deploy_runs import (get_inputs, deploy_runs, _replace_in_cmnd,
                                    _create_qsub,
                                    deploy_run_normal, deploy_run_qsub)

logging.basicConfig(level="INFO")

example_run = """runs:
  - id: 1
    name: run_test_name
    environment: unix
    
    input:
        scripts:
          - input/script.in
        variables:
          var1: value
        files:
          frag1: input/frag.in
          other: input/other.in
        remote:
            hostname: {host}
            username: user
            port: {port}
            password: password
    
    process:
        unix:
          run:
            - echo test_echo > output.txt
            - cat script.in > output2.txt
            - mkdir subfolder; echo a > subfolder/to_delete.txt; echo b > subfolder/dont_delete.txt
            - mkdir deletefolder; echo c > deletefolder/some.text
        qsub:
            walltime: 1:10
            modules:
                - quantum-espresso
                - intel-suite
                - mpi
            run: 
                - echo test_echo > output.txt
                - cat script.in > output2.txt
                - mkdir subfolder; echo a > subfolder/to_delete.txt; echo b > subfolder/dont_delete.txt
                - mkdir deletefolder; echo c > deletefolder/some.text
            
    output:
      remote:
         hostname: {host}
         username: user
         port: {port}
         password: password
      remove:
        - "*/to_delete.txt"
        - deletefolder
        - tmp.*
      rename:
        2.txt: 2.other

"""


@pytest.fixture("function")
def local_pathlib():

    test_folder = os.path.join(os.path.dirname(__file__), 'test_tmp')
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.mkdir(test_folder)

    configpath = os.path.join(test_folder, "config.yml")
    with open(configpath, "w") as f:
        f.write(example_run.format(host="null", port=1))
    inpath = os.path.join(test_folder, "input")
    os.mkdir(inpath)
    with open(os.path.join(inpath, 'script.in'), 'w') as f:
        f.write('test @v{var1} @f{frag1}')
    with open(os.path.join(inpath, 'frag.in'), 'w') as f:
        f.write('replace frag')
    with open(os.path.join(inpath, 'other.in'), 'w') as f:
        f.write('another file')

    yield format_config_yaml(configpath), test_folder


@pytest.fixture("function")
def local_mock():
    config_file = MockPath('config.yml', is_file=True,
                           content=example_run.format(host="null", port=1))
    test_folder = MockPath(
        "test_tmp",
        structure=[config_file,
                   {"input": [
                       MockPath(
                           'script.in', is_file=True,
                           content='test @v{var1} @f{frag1}'),
                       MockPath('frag.in', is_file=True,
                                content='replace frag'),
                       MockPath(
                           'other.in', is_file=True,
                           content='another file'),
                   ]}])

    yield format_config_yaml(config_file), test_folder


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
        f.write('test @v{var1} @f{frag1}')
    with open(os.path.join(inpath, 'frag.in'), 'w') as f:
        f.write('replace frag')
    with open(os.path.join(inpath, 'other.in'), 'w') as f:
        f.write('another file')

    with mockserver.Server({"user": {"password": "password"}}, test_folder) as server:

        with open(configpath, "w") as f:
            f.write(example_run.format(host=server.host, port=server.port))

        yield format_config_yaml(configpath), test_folder


# a better way to do this is in the works:
# https://docs.pytest.org/en/latest/proposals/parametrize_with_fixtures.html
@pytest.fixture(params=['local_pathlib', 'local_mock', 'remote'])
def context(request):
    return request.getfixturevalue(request.param)


def test_get_inputs(context):
    runs, path = context
    inputs = get_inputs(runs[0], path)
    assert sorted(list(inputs["files"].keys())) == ['frag.in', 'other.in']
    assert list(inputs["scripts"].keys()) == ["script.in"]
    assert inputs["cmnds"] == [
        "echo test_echo > output.txt",
        "cat script.in > output2.txt",
        "mkdir subfolder; echo a > subfolder/to_delete.txt; echo b > subfolder/dont_delete.txt",
        "mkdir deletefolder; echo c > deletefolder/some.text"]


def test_get_inputs_missing_variable_in_script(context):
    runs, path = context
    run = runs[0]
    run["input"]["variables"] = {}
    with pytest.raises(KeyError):
        _ = get_inputs(run, path)


def test_get_inputs_missing_file_in_script(context):
    runs, path = context
    run = runs[0]
    run["input"]["files"] = {}
    with pytest.raises(KeyError):
        _ = get_inputs(run, path)


def test_get_inputs_missing_file(context):
    runs, path = context
    run = runs[0]
    run["input"]["files"] = {"other_file": "other_file.in"}
    with pytest.raises(ValueError):
        _ = get_inputs(run, path)


def test_replace_in_cmnd():
    cmnd = _replace_in_cmnd(
        "mpirun -np @v{nprocs} script.in -a @v{other} > file.out",
        {"nprocs": 2, "other": "val"}, 1)
    assert cmnd == "mpirun -np 2 script.in -a val > file.out"

    with pytest.raises(KeyError):
        _replace_in_cmnd("mpirun -np @v{nprocs} script.in > file.out", {}, 1)


def test_run_deploy_normal(context):
    runs, path = context
    inputs = get_inputs(runs[0], path)
    deploy_run_normal(runs[0], inputs, path)

    if not hasattr(path, "to_string"):
        assert os.path.exists(os.path.join(
            str(path), 'output/1_run_test_name/output.txt'))
        assert os.path.exists(os.path.join(
            str(path), 'output/1_run_test_name/output2.other'))
    else:
        expected = """Folder("test_tmp")
  File("config.yml")
  Folder("input")
    File("frag.in")
    File("other.in")
    File("script.in")
  Folder("output")
    Folder("1_run_test_name")
      File("config_1.yaml")
      File("frag.in")
      File("other.in")
      File("output.txt")
      File("output2.other")
      File("script.in")
      Folder("subfolder")
        File("dont_delete.txt")"""
        assert path.to_string(file_content=False) == expected

        assert path["output/1_run_test_name/script.in"]._content == ["test value replace frag"]


def test_full_normal(context):
    runs, path = context
    deploy_runs(runs, path, if_exists="abort", exec_errors=True)


def test_create_qsub(context):
    runs, path = context

    inputs = get_inputs(runs[0], path)

    out = _create_qsub(runs[0], "path/to/dir", inputs["cmnds"])

    expected = """#!/bin/bash --login
#PBS -N 1_run_test_nam
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
readlink -f "." &> /dev/null || readlink_fail=true
if [[ ! "$readlink_fail" = true ]]; then
export PBS_O_WORKDIR=$(readlink -f $PBS_O_WORKDIR)
else
export PBS_O_WORKDIR=$(readlink $PBS_O_WORKDIR)
fi

# Set the number of threads to 1
#   This prevents any system libraries from automatically 
#   using threading.
export OMP_NUM_THREADS=1

echo Running: 1_run_test_name

# load required modules
module load quantum-espresso intel-suite mpi

start_in_temp=true

if [ "$start_in_temp" = true ] ; then

    if [ -z ${TMPDIR+x} ]; then 
        echo "the TMPDIR variable does not exist"  1>&2
        exit 1
    fi
    if [ -z "$TMPDIR" ]; then
        echo "the TMPDIR variable is empty"  1>&2
        exit 1
    fi
    echo "running in: $TMPDIR"
    cd $TMPDIR
    
    # copy required input files from $WORKDIR to $TMPDIR
    # if running on multiple nodes, then the files need to be copied to each one
    if [ ! -z ${PBS_NODEFILE+x} ]; then
        echo '$PBS_NODEFILE' found: $PBS_NODEFILE

        readarray -t PCLIST < $PBS_NODEFILE
        # get unique items
        IFS=$' '
        PCLIST=($(printf "%s\n" "${PCLIST[@]}" | sort -u | tr '\n' ' '))
        unset IFS
        # echo "running on nodes: ${PCLIST[*]}"

        for PC in "${PCLIST[@]}"; do
            echo "copying input files to node $PC"
            ssh $PC "if [ ! -d $TMPDIR ];then mkdir -p $TMPDIR;echo 'temporary directory on '$PC;fi"
            ssh $PC cp -pR path/to/dir/* $TMPDIR
            # echo `ssh $PC ls $TMPDIR`
        done
    else
        cp -pR path/to/dir/* $TMPDIR
    fi

else

    echo "running in: path/to/dir"
    cd path/to/dir
    
fi

# main commands to run
echo test_echo > output.txt
cat script.in > output2.txt
mkdir subfolder; echo a > subfolder/to_delete.txt; echo b > subfolder/dont_delete.txt
mkdir deletefolder; echo c > deletefolder/some.text

# remove output files
for path in $(find */to_delete.txt); do if [ -e $path ]; then rm -Rf $path; fi; done
for path in $(find deletefolder); do if [ -e $path ]; then rm -Rf $path; fi; done
for path in $(find tmp.*); do if [ -e $path ]; then rm -Rf $path; fi; done

# rename output files
find . -depth -name '*2.txt*' -execdir bash -c 'mv -i "$1" "${1//2.txt/2.other}"' bash {} \;

if [ "$start_in_temp" = true ] ; then

    # copy output files from $TMPDIR to $WORKDIR
    cp -pR $TMPDIR/* path/to/dir
    
    cd path/to/dir
    
fi

"""
    # print(out)
    assert out == expected


def test_run_deploy_qsub_fail_local(local_pathlib):
    runs, path = local_pathlib
    run = runs[0]
    run["environment"] = "qsub"
    inputs = get_inputs(run, path)

    assert deploy_run_qsub(runs[0], inputs, path, exec_errors=True) == False


def test_run_deploy_qsub_pass_local(local_pathlib):

    runs, path = local_pathlib
    run = runs[0]
    run["environment"] = "qsub"
    inputs = get_inputs(run, path)

    # Travis doesn't allow setting global variable in command line
    if os.getenv('CI', False) and os.getenv('TMPDIR', False):
        if not os.path.exists(os.environ['TMPDIR']):
            os.makedirs(os.environ['TMPDIR'])
        temppath = mkdtemp(dir=os.environ['TMPDIR'])
    else:
        temppath = mkdtemp()
    try:
        with mock.patch("atomic_hpc.deploy_runs._QSUB_CMNDLINE",
                        "TMPDIR={0}; chmod +x run.qsub; ./run.qsub".format(str(temppath))):
            assert deploy_run_qsub(
                runs[0], inputs, path, exec_errors=True) == True
    finally:
        shutil.rmtree(temppath)

    # outfile = pathlib.Path(os.path.join(str(path), 'output/1_run_test_name/run.qsub'))
    # with outfile.open() as f:
    #     assert "TMPDIR={0}; chmod +x run.qsub; ./run.qsub".format(str(temppath)) == f.read()

    outpath = pathlib.Path(os.path.join(str(path), 'output/1_run_test_name'))
    assert outpath.exists()
    expected = ['output/1_run_test_name/config_1.yaml', 'output/1_run_test_name/frag.in',
                'output/1_run_test_name/other.in', 'output/1_run_test_name/output.txt',
                'output/1_run_test_name/output2.other', 'output/1_run_test_name/run.qsub',
                'output/1_run_test_name/script.in', 'output/1_run_test_name/subfolder',
                'output/1_run_test_name/subfolder/dont_delete.txt']
    assert sorted([str(p.relative_to(path))
                   for p in outpath.glob("**/*")]) == sorted(expected)

    outfile = pathlib.Path(os.path.join(
        str(path), 'output/1_run_test_name/output2.other'))
    with outfile.open() as f:
        assert "test value replace frag" == f.read()


def test_run_deploy_qsub_pass_remote(remote):

    runs, path = remote
    run = runs[0]
    run["environment"] = "qsub"
    inputs = get_inputs(run, path)

    # Travis doesn't allow setting global variable in command line
    if os.getenv('CI', False) and os.getenv('TMPDIR', False):
        if not os.path.exists(os.environ['TMPDIR']):
            os.makedirs(os.environ['TMPDIR'])
        temppath = mkdtemp(dir=os.environ['TMPDIR'])
    else:
        temppath = mkdtemp()
    try:
        with mock.patch("atomic_hpc.deploy_runs._QSUB_CMNDLINE",
                        "TMPDIR={0}; chmod +x run.qsub; ./run.qsub".format(str(temppath))):
            assert deploy_run_qsub(
                runs[0], inputs, path, exec_errors=True) == True
    finally:
        shutil.rmtree(temppath)

    # outfile = pathlib.Path(os.path.join(str(path), 'output/1_run_test_name/run.qsub'))
    # with outfile.open() as f:
    #     assert "TMPDIR={0}; chmod +x run.qsub; ./run.qsub".format(str(temppath)) == f.read()

    outpath = pathlib.Path(os.path.join(str(path), 'output/1_run_test_name'))
    assert outpath.exists()
    expected = ['output/1_run_test_name/config_1.yaml', 'output/1_run_test_name/frag.in',
                'output/1_run_test_name/other.in', 'output/1_run_test_name/output.txt',
                'output/1_run_test_name/output2.other', 'output/1_run_test_name/run.qsub',
                'output/1_run_test_name/script.in', 'output/1_run_test_name/subfolder',
                'output/1_run_test_name/subfolder/dont_delete.txt']
    assert sorted([str(p.relative_to(path))
                   for p in outpath.glob("**/*")]) == sorted(expected)

    outfile = pathlib.Path(os.path.join(
        str(path), 'output/1_run_test_name/output2.other'))
    with outfile.open() as f:
        assert "test value replace frag" == f.read()
