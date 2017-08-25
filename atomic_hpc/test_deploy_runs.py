import os
import shutil
import pytest
from jsonextended import utils
from atomic_hpc.config_yaml import runs_from_config
from atomic_hpc.deploy_runs import _create_scripts, _create_qsub, deploy_runs

example_file_local = """
runs:
  - id: 1
    name: run_local
    environment: local
    scripts:
      - input/script.in
    variables:
      var1: value
    files:
      frag1: input/frag.in
    local:
      run:
        - echo test_echo > output.txt
    cleanup:
      remove:
        - frag.in
      aliases:
        .txt: .other
  - id: 2
    name: run_local_child
    environment: local
    local:
      run:
        - echo test_echo2 > output2.txt
    requires: 1
"""

example_file_qsub = """
runs:
  - id: 1
    name: run_qsub
    environment: qsub
    qsub:
      walltime: 1:10
    scripts:
      - input/script.in
    variables:
      var1: value
    files:
      frag1: input/frag.in
    
"""

if os.path.exists('test_tmp'):
    shutil.rmtree('test_tmp')
os.makedirs('test_tmp/input')


@pytest.fixture("function")
def config_qsub():

    file_obj = utils.MockPath('test_tmp/config.yml', is_file=True,
                              content=example_file_qsub)

    yield runs_from_config(file_obj)

@pytest.fixture("function")
def config_local():

    file_obj = utils.MockPath('test_tmp/config.yml', is_file=True,
                              content=example_file_local)

    yield runs_from_config(file_obj)


def test_create_scripts_fails_variable(config_qsub):

    top_level, config_dir = config_qsub

    with open('test_tmp/input/script.in', 'w') as f:
        f.write('test @v{missingvar}')
    with pytest.raises(KeyError, message="Expecting KeyError"):
        _create_scripts(top_level[0], config_dir)


def test_create_fails_file(config_qsub):
    top_level, config_dir = config_qsub

    with open('test_tmp/input/script.in', 'w') as f:
        f.write('test @f{missingvar}')
    with pytest.raises(KeyError, message="Expecting KeyError"):
        _create_scripts(top_level[0], config_dir)


def test_create_scripts_runs(config_qsub):
    top_level, config_dir = config_qsub

    with open('test_tmp/input/script.in', 'w') as f:
        f.write('test @v{var1}\n @f{frag1}')
    with open('test_tmp/input/frag.in', 'w') as f:
        f.write('replace\n frag')

    output = _create_scripts(top_level[0], config_dir)
    scriptname, script = output[0]
    assert scriptname == "script.in"
    assert script == "test value\n replace\n frag"


def test_create_qsub(config_qsub):
    top_level, config_dir = config_qsub

    out = _create_qsub(top_level[0]["qsub"], 1, "test")
    assert "PBS -N 1_test" in out
    assert "#PBS -l walltime=1:10:00" in out


def test_deploy_local(config_local):
    top_level, config_dir = config_local

    with open('test_tmp/input/script.in', 'w') as f:
        f.write('test @v{var1}\n @f{frag1}')
    with open('test_tmp/input/frag.in', 'w') as f:
        f.write('replace\n frag')

    deploy_runs(top_level, config_dir, separate_dir=False)
    assert os.path.exists('test_tmp/output/1_run_local/output.other')
    assert os.path.exists('test_tmp/output/1_run_local/output2.txt')


def test_deploy_local_separate(config_local):
    top_level, config_dir = config_local

    with open('test_tmp/input/script.in', 'w') as f:
        f.write('test @v{var1}\n @f{frag1}')
    with open('test_tmp/input/frag.in', 'w') as f:
        f.write('replace\n frag')

    deploy_runs(top_level, config_dir, separate_dir=True)
    assert os.path.exists('test_tmp/output/1_run_local/output.other')
    assert os.path.exists('test_tmp/output/2_run_local_child/output2.txt')

