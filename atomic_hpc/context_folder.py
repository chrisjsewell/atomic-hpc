""" a module to create a context manager for opening a
folder, that can be either local, virtual or remote

"""
import os
import shutil
from subprocess import Popen, PIPE, STDOUT
import logging
from contextlib import contextmanager

import paramiko

# python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str
# python 3 to 2 compatibility
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib


def splitall(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


class VirtualDir(object):
    """ a virtual directory with implementation agnostic methods

    """
    def exists(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        raise NotImplementedError

    def isfile(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        raise NotImplementedError

    def isdir(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        raise NotImplementedError

    @contextmanager
    def open(self, path, mode='r', encoding=None):
        """

        Parameters
        ----------
        path
        mode
        encoding

        Returns
        -------

        """
        raise NotImplementedError

    def makedirs(self, path):
        """

        Parameters
        ----------
        path

        Returns
        -------

        """
        raise NotImplemented

    def rmtree(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        raise NotImplementedError

    def remove(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        raise NotImplementedError

    def rename(self, path, newname):
        """

        Parameters
        ----------
        path: str
        newname: str

        Returns
        -------

        """
        raise NotImplementedError

    def glob(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        raise NotImplementedError

    def copy(self, inpath, outpath):
        """

        Parameters
        ----------
        inpath
        outpath

        Returns
        -------

        """
        raise NotImplementedError

    def copy_from(self, path, source):
        """

        Parameters
        ----------
        path: str
        source: str or path_like
            or can be a class with a `copy_from` method

        Returns
        -------

        """
        raise NotImplementedError

    def copy_to(self, path, target):
        """ copy to a local target

        Parameters
        ----------
        path: str
        target: str or path_like
            or can be a class with a `copy_to` method

        Returns
        -------

        """
        raise NotImplementedError

    def exec_cmnd(self, path, cmnd):
        """

        Parameters
        ----------
        path: str
        cmnd: str

        Returns
        -------

        """
        raise NotImplementedError


class LocalPath(VirtualDir):
    def __init__(self, root):
        """

        Parameters
        ----------
        root: pathlib.Path

        """
        self.root = root

    def exists(self, path):
        """ whether path exists

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        path = self.root.joinpath(path)
        return path.exists()

    def isfile(self, path):
        """ whether path is a file

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        path = self.root.joinpath(path)
        return path.is_file()

    def isdir(self, path):
        """ whether path is a directory

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        path = self.root.joinpath(path)
        return path.is_dir()

    def makedirs(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        parts = splitall(path)
        newpath = self.root.joinpath(parts[0])
        if not newpath.exists():
            newpath.mkdir()
        for part in parts[1:]:
            newpath = newpath.joinpath(part)
            if not newpath.exists():
                newpath.mkdir()

    def rmtree(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        delpath = self.root.joinpath(path)
        if self.root.samefile(delpath):
            raise IOError("attempting to remove the root directory")
        if not delpath.exists():
            raise IOError("root doesn't exist: {}".format(path))
        if not delpath.is_dir():
            raise IOError("root is not a directory: {}".format(path))
        for subpath in delpath.glob("**/*"):
            if subpath.is_file():
                subpath.unlink()
        for subpath in reversed(list(delpath.glob("**"))):
            subpath.rmdir()
        if delpath.exists():
            delpath.rmdir()

    def remove(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        delpath = self.root.joinpath(path)
        delpath.unlink()

    def rename(self, path, newname):
        """

        Parameters
        ----------
        path: str
        newname: str

        Returns
        -------

        """
        path = self.root.joinpath(path)
        newname = path.parent.joinpath(newname)
        path.rename(newname)

    @contextmanager
    def open(self, path, mode='r', encoding=None):
        """ open a file, it will be created if it does not initially exist

        Parameters
        ----------
        path: str
        mode: str
        encoding: str

        Returns
        -------

        """
        path = self.root.joinpath(path)
        if not path.exists():
            path.touch()
        if not path.is_file():
            raise IOError("path is not a file: {}".format(path))
        with path.open(mode=mode, encoding=encoding) as f:
            yield f

    def glob(self, pattern):
        """

        Parameters
        ----------
        pattern: str

        Returns
        -------

        """
        return self.root.glob(pattern)

    def copy(self, inpath, outpath):
        """ copy one path to another, where both are internal to the context path

        the entire contents of infile will be copied into outpath

        Parameters
        ----------
        inpath: str
        outpath: str
        # contentonly: bool
        #    if True and inpath is a dir, copy only the content of the inpath, otherwise copy the entire folder

        Returns
        -------

        """
        outpath = self.root.joinpath(outpath)
        inpath = self.root.joinpath(inpath)
        if not inpath.exists():
            raise IOError("the inpath does not exist: {}".format(inpath))
        if not outpath.exists():
            raise IOError("the outpath does not exist: {}".format(outpath))
        if hasattr(inpath, "copy_path_obj"):
            inpath = inpath.copy_path_obj()
        self.copy_from(inpath, os.path.relpath(str(outpath), str(self.root)))

    def copy_from(self, source, path):
        """ copy from a local source outside the context folder

        Parameters
        ----------
        path: str
        source: str or path_like
            or can be a class with a `copy_from` method

        Returns
        -------

        """
        subpath = self.root.joinpath(path)

        if hasattr(subpath, "copy_from"):
            subpath.copy_from(source)
            return

        source = pathlib.Path(source)
        if source.is_file() and source.exists():
            shutil.copy(str(source), str(subpath.joinpath(source.name)))
        elif source.is_dir() and source.exists():
            shutil.copytree(str(source), str(subpath.joinpath(source.name)))
        else:
            raise IOError("the source is not an existing file or directory: {}".format(source))

    def copy_to(self, path, target):
        """ copy to a local target outside the context folder

        Parameters
        ----------
        path: str
        target: str or path_like
            or can be a class with a `copy_to` method

        Returns
        -------

        """
        subpath = self.root.joinpath(path)

        if hasattr(subpath, "copy_to"):
            subpath.copy_to(target)
            return

        target = pathlib.Path(target)
        if target.is_file() and target.exists():
            shutil.copy(str(subpath), str(target.joinpath(subpath.name)))
        elif target.is_dir() and target.exists():
            shutil.copytree(str(subpath), str(target.joinpath(subpath.name)))
        else:
            raise IOError("the target is not an existing file or directory")

    @staticmethod
    def _log_output(pipe):
        for line in iter(pipe.readline, b''):
            logging.info('{}'.format(line.decode("utf-8").strip()))

    @contextmanager
    def _exec_in_dir(self, path):
        previous_path = os.getcwd()
        if hasattr(path, "maketemp"):
            with path.maketemp(getoutput=True) as tempdir:
                try:
                    os.chdir(str(tempdir))
                    yield
                finally:
                    os.chdir(previous_path)
        else:
            try:
                os.chdir(str(path))
                yield
            finally:
                os.chdir(previous_path)

    def exec_cmnd(self, cmnd, path='.', raise_error=True):
        """ perform a command line execution

        Parameters
        ----------
        cmnd: str
        path: str
        raise_error: True

        Returns
        -------

        """
        runpath = self.root.joinpath(path)
        runpath.absolute()
        with self._exec_in_dir(runpath):
            # subprocess.run(cmnd, shell=True, check=True)
            process = Popen(cmnd, stdout=PIPE, stderr=STDOUT, shell=True)
            with process.stdout:
                self._log_output(process.stdout)
            exitcode = process.wait()  # 0 means success
            if exitcode:
                err_msg = "the following line in caused error code {1}: {2}".format(exitcode, cmnd)
                logging.error(err_msg)
                if raise_error:
                    raise RuntimeError(err_msg)


class RemotePath(VirtualDir):
    def __init__(self, ssh, sftp):
        """

        Parameters
        ----------
        ssh: paramiko.client.SSHClient
        sftp: paramiko.sftp_client.SFTPClient

        """
        self.ssh = ssh
        self.sftp = sftp

class change_dir(object):
    """Context manager for changing the current working directory"""

    def __init__(self, path, remote=False, hostname='', username=None, password=None, **kwargs):
        """

        Parameters
        ----------
        path: str or path_type
        remote: bool
        hostname: str
            the server to connect to
        username: str
            the username to authenticate as (defaults to the current local username)
        password: str
            a password to use for authentication or for unlocking a private key
        kwargs:
            additional keyword arguments for paramiko.client.SSHClient.connect

        """

        if not remote:
            self._remote = False
            if isinstance(path, basestring):
                if not os.path.exists(path):
                    raise IOError("the path does not exist: {}".format(path))
                if not os.path.isdir(path):
                    raise IOError("the path is not a directory: {}".format(path))
                abspath = os.path.expanduser(os.path.abspath(path))
                self._path = pathlib.Path(abspath)
            else:
                if not hasattr(path, "is_dir"):
                    raise IOError("path is not path_like: {}".format(path))
                if not path.exists():
                    raise IOError("the path does not exist: {}".format(path))
                if not path.is_dir():
                    raise IOError("path is not a directory: {}".format(path))
                self._path = path  # .absolute()
        else:
            self._remote = True
            if not isinstance(path, basestring):
                raise IOError("the path must be a string: {}".format(path))
            self._path = path
            self._hostname = ''
            self._username = None
            self._password = None
            self._kwargs = kwargs

    def __enter__(self):
        if not self._remote:
            return LocalPath(self._path)
        else:
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh.connect(self._hostname, username=self._username, password=self._password, **self._kwargs)
            self._sftp = self._ssh.open_sftp()
            self._sftp.chdir(self._path)
            return RemotePath(self._ssh, self._sftp)

    def __exit__(self, etype, value, traceback):
        if self._remote:
            try:
                self._ssh.close()
            except:
                pass
