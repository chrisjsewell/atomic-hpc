import os
import shutil
import pytest
import inspect
import logging

import sys
from jsonextended.utils import MockPath
from atomic_hpc.context_folder import change_dir, LocalPath, RemotePath
from atomic_hpc.mockssh import mockserver
# python 3 to 2 compatibility
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib


def test_consistent():
    local = [name for name, val in inspect.getmembers(LocalPath, predicate=inspect.isfunction) if not name.startswith("_")]
    remote = [name for name, val in inspect.getmembers(RemotePath, predicate=inspect.isfunction) if not name.startswith("_")]
    assert sorted(local) == sorted(remote)


@pytest.fixture("function")
def local_pathlib():

    test_folder = os.path.join(os.path.dirname(__file__), 'test_tmp')
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.mkdir(test_folder)
    test_file = os.path.join(test_folder, "file.txt")
    with open(test_file, "w") as f:
        f.write("file content")
    dir1 = pathlib.Path(test_folder)

    test_external = os.path.join(os.path.dirname(__file__), 'test_external')
    if os.path.exists(test_external):
        shutil.rmtree(test_external)
    os.mkdir(test_external)
    test_file = os.path.join(test_external, "file.txt")
    with open(test_file, "w") as f:
        f.write("file content")
    test_external = pathlib.Path(test_external)

    with change_dir(dir1) as testdir:
        yield testdir, test_external


@pytest.fixture("function")
def local_mockpath():

    file1 = MockPath('file.txt', is_file=True,
                     content="file content")
    dir1 = MockPath('test_tmp', structure=[file1])
    dir2 = MockPath('test_external', structure=[file1.copy_path_obj()])

    with change_dir(dir1) as testdir:
        yield testdir, dir2


@pytest.fixture("function")
def remote():
    test_folder = os.path.join(os.path.dirname(__file__), 'test_tmp')
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.mkdir(test_folder)
    test_file = os.path.join(test_folder, "file.txt")
    with open(test_file, "w") as f:
        f.write("file content")

    test_external = os.path.join(os.path.dirname(__file__), 'test_external')
    if os.path.exists(test_external):
        shutil.rmtree(test_external)
    os.mkdir(test_external)
    test_file = os.path.join(test_external, "file.txt")
    with open(test_file, "w") as f:
        f.write("file content")
    test_external = pathlib.Path(test_external)

    with mockserver.Server({"user": {"password": "password"}}, test_folder) as server:
        with change_dir(".", remote=True, hostname=server.host,
                        port=server.port, username="user", password="password") as testdir:
            yield testdir, test_external


# a better way to do this is in the works: https://docs.pytest.org/en/latest/proposals/parametrize_with_fixtures.html
@pytest.fixture(params=['local_pathlib', 'local_mockpath', 'remote'])
def context(request):
    return request.getfuncargvalue(request.param)


def test_context_methods1(context):
    testdir, test_external = context

    assert testdir.exists('file.txt')
    assert testdir.isfile('file.txt')
    testdir.makedirs('subdir1/subsubdir1')

    assert sorted([p for p in testdir.glob('*')]) == sorted(['file.txt', 'subdir1'])
    assert sorted([p for p in testdir.glob('subdir1/*')]) == sorted(['subdir1/subsubdir1'])
    assert sorted([p for p in testdir.glob('**')]) == sorted(['subdir1', 'subdir1/subsubdir1'])
    assert sorted([p for p in testdir.glob('**/*')]) == sorted(['file.txt', 'subdir1',
                                                                'subdir1/subsubdir1'])
    assert testdir.exists('subdir1/subsubdir1')
    assert testdir.isdir('subdir1/subsubdir1')

    testdir.rmtree('subdir1')
    assert sorted([p for p in testdir.glob('**/*')]) == sorted(['file.txt'])


def test_context_methods2(context):
    testdir, test_external = context

    testdir.rename('file.txt', 'file2.txt')
    assert sorted([p for p in testdir.glob('**/*')]) == sorted(['file2.txt'])
    testdir.remove('file2.txt')
    assert [p for p in testdir.glob('**/*')] == []
    testdir.copy_from(test_external, '.')
    assert sorted([p for p in testdir.glob('**/*')]) == sorted(['test_external', 'test_external/file.txt'])
    testdir.copy_to('.', test_external)
    expected = sorted(['test_tmp', 'file.txt', 'test_tmp/test_external', 'test_tmp/test_external/file.txt'])
    assert sorted([str(p.relative_to(test_external)) for p in test_external.glob('**/*')]) == expected
    testdir.exec_cmnd("echo test123 >> new.txt", "test_external")
    assert testdir.exists("test_external/new.txt")

    with testdir.open("test_external/new.txt") as f:
        assert f.read().strip() == "test123"

    testdir.copy("test_external/new.txt", ".")
    expected = sorted(['new.txt', 'test_external', 'test_external/file.txt', 'test_external/new.txt'])
    assert sorted(list(testdir.glob('**/*'))) == expected

    testdir.makedirs("other")
    testdir.copy("test_external", "other")
    expected = sorted(['new.txt', 'test_external', 'test_external/file.txt', 'test_external/new.txt',
                       'other', 'other/test_external', 'other/test_external/file.txt', 'other/test_external/new.txt'])
    assert sorted(list(testdir.glob('**/*'))) == expected

  #       expected_output = """Folder("dir1")
  # Folder("dir2")
  #   File("file.txt") Contents:
  #    file content
  # File("new.txt") Contents:
  #  hi"""
  #       assert dir1.to_string(file_content=True) == expected_output


def test_exec_fail(context):
    testdir, _ = context

    testdir.exec_cmnd("kjblkblkjb", raise_error=False)
    with pytest.raises(RuntimeError):
        testdir.exec_cmnd("kjblkblkjb", raise_error=True)


def test_exec_longer_cmnd(context):
    testdir, _ = context
    # TODO timeout not working with local, maybe use something like this: https://stackoverflow.com/a/4825933/5033292
    # with pytest.raises(Exception):
    #     testdir.exec_cmnd("sleep 6", timeout=1)
    testdir.exec_cmnd("sleep 5")


def test_exec_cmnd_with_stderr(context):
    testdir, _ = context
    testdir.exec_cmnd("echo This message goes to stdout >&1")
    testdir.exec_cmnd("echo This message goes to stderr >&2")


def test_exec_cmnd_multiline_output(context):
    testdir, _ = context
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO, stream=sys.stdout)
    assert testdir.exec_cmnd('bash -c \'for ((i = 0 ; i < 4 ; i++ )); do echo "abc" >&1; echo "efg" >&2; sleep 1; done\'')



