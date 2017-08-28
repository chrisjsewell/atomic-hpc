import pytest
import inspect
from jsonextended.utils import MockPath
from atomic_hpc.context_folder import change_dir, splitall, LocalPath, RemotePath
# import pathlib


def test_splitall():
    assert splitall('a/b/c') == ['a', 'b', 'c']


def test_consistent():
    local = [name for name, val in inspect.getmembers(LocalPath, predicate=inspect.ismethod) if not name.startswith("_")]
    remote = [name for name, val in inspect.getmembers(RemotePath, predicate=inspect.ismethod) if not name.startswith("_")]
    assert sorted(local) == sorted(remote)


def test_local():

    file1 = MockPath('file.txt', is_file=True,
                     content="file content")
    dir1 = MockPath('dir1', structure=[file1])
    dir2 = MockPath('dir2', structure=[file1.copy_path_obj()])

    # dir1 = pathlib.Path("dir1")
    # dir2 = pathlib.Path("dir2")

    with change_dir(dir1) as testdir:
        assert testdir.exists('file.txt')
        assert testdir.isfile('file.txt')
        testdir.makedirs('subdir1/subsubdir1')
        assert sorted([str(p) for p in testdir.glob('**/*')]) == sorted(['dir1/file.txt', 'dir1/subdir1',
                                                                         'dir1/subdir1/subsubdir1'])
        assert testdir.exists('subdir1/subsubdir1')
        assert testdir.isdir('subdir1/subsubdir1')
        testdir.rmtree('subdir1')
        assert sorted([str(p) for p in testdir.glob('**/*')]) == sorted(['dir1/file.txt'])
        testdir.rename('file.txt', 'file2.txt')
        assert sorted([str(p) for p in testdir.glob('**/*')]) == sorted(['dir1/file2.txt'])
        testdir.remove('file2.txt')
        assert [str(p) for p in testdir.glob('**/*')] == []
        testdir.copy_from(dir2, '.')
        assert sorted([str(p) for p in testdir.glob('**/*')]) == sorted(['dir1/dir2', 'dir1/dir2/file.txt'])
        testdir.copy_to('.', dir2)
        assert sorted([str(p) for p in dir2.glob('**/*')]) == sorted(['dir2/dir1', 'dir2/file.txt',
                                                                      'dir2/dir1/dir2', 'dir2/dir1/dir2/file.txt'])
        testdir.exec_cmnd("echo hi >> new.txt")
        assert dir1.joinpath("new.txt").exists()
        with dir1.joinpath("new.txt").open() as f:
            assert f.read() == "hi"
