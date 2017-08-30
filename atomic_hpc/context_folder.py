""" a module to create a context manager for opening a
folder, that can be either local, virtual or remote

"""
import codecs
import os
import shutil
import stat
from fnmatch import fnmatch
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
    """ the abstract class for a virtual directory with implementation agnostic methods

    """

    @staticmethod
    def name(path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        return os.path.basename(str(path))

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

    def getabs(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------
        abspath: str

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

    def glob(self, pattern):
        """

        Parameters
        ----------
        pattern: str

        Yields
        -------
        path: str
            the path relative to the root

        """
        raise NotImplementedError

    def iterdir(self):
        """

        Yields
        -------
        subpath:
            each subpath in the folder

        """
        for path in self.glob("*"):
            yield path

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
        self._root = root

    def exists(self, path):
        """ whether path exists

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        path = self._root.joinpath(path)
        return path.exists()

    def isfile(self, path):
        """ whether path is a file

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        path = self._root.joinpath(path)
        return path.is_file()

    def isdir(self, path):
        """ whether path is a directory

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        path = self._root.joinpath(path)
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
        newpath = self._root.joinpath(parts[0])
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
        delpath = self._root.joinpath(path)
        if self._root.samefile(delpath):
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
        delpath = self._root.joinpath(path)
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
        path = self._root.joinpath(path)
        newname = path.parent.joinpath(newname)
        path.rename(newname)

    def getabs(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------
        abspath: str

        """
        path = self._root.joinpath(path)
        return str(path.absolute())

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
        path = self._root.joinpath(path)
        if not path.exists():
            path.touch()
        if not path.is_file():
            raise IOError("path is not a file: {}".format(path))
        with path.open(mode=mode, encoding=encoding) as f:
            yield f

    def glob(self, pattern):
        """ yield all path that match the pattern in the directory
        (not including the root)

        Parameters
        ----------
        pattern: str

        Yields
        -------
        path: str
            path relative to root

        """
        for path in self._root.glob(pattern):
            if not path.samefile(self._root):
                yield str(path.relative_to(self._root))

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
        outpath = self._root.joinpath(outpath)
        inpath = self._root.joinpath(inpath)
        if not inpath.exists():
            raise IOError("the inpath does not exist: {}".format(inpath))
        if not outpath.exists():
            raise IOError("the outpath does not exist: {}".format(outpath))
        if hasattr(inpath, "copy_path_obj"):
            inpath = inpath.copy_path_obj()
        self.copy_from(inpath, os.path.relpath(str(outpath), str(self._root)))

    def copy_from(self, source, path):
        """ copy from a local source outside the context folder

        Parameters
        ----------
        path: str
        source: str or path_like

        Returns
        -------

        """
        subpath = self._root.joinpath(path)

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

        Returns
        -------

        """
        subpath = self._root.joinpath(path)

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

    # TODO get stderror message
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
        runpath = self._root.joinpath(path)
        runpath.absolute()
        with self._exec_in_dir(runpath):
            # subprocess.run(cmnd, shell=True, check=True)
            process = Popen(cmnd, stdout=PIPE, stderr=STDOUT, shell=True)
            with process.stdout:
                self._log_output(process.stdout)
            exitcode = process.wait()  # 0 means success
            if exitcode:
                err_msg = "the following line in caused error code {0}: {1}".format(exitcode, cmnd)
                logging.error(err_msg)
                if raise_error:
                    raise RuntimeError(err_msg)


def renew_connection(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self._ssh.get_transport().is_active():
            self._ssh.connect(self._hostname, **self._kwargs)
            self._sftp = self._ssh.open_sftp()
            self._sftp.chdir(self._root)
        return func(*args, **kwargs)
    return wrapper


class RemotePath(VirtualDir):

    def __init__(self, ssh, hostname, path, **kwargs):
        """

        Parameters
        ----------
        ssh: paramiko.client.SSHClient
        path: str

        """
        self._root = path
        self._ssh = ssh
        self._hostname = hostname
        self._kwargs = kwargs
        if "allow_agent" not in kwargs:
            self._kwargs["allow_agent"] = False
        if "look_for_keys" not in kwargs:
            self._kwargs["look_for_keys"] = False
        self._ssh.connect(self._hostname, **self._kwargs)
        self._sftp = self._ssh.open_sftp()
        if not self.exists(self._root):
            self.makedirs(self._root)
        self._sftp.chdir(self._root)

    @renew_connection
    def exists(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        try:
            self._sftp.stat(path)
            return True
        except IOError:
            return False

    @renew_connection
    def isdir(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        return stat.S_ISDIR(self._sftp.stat(path).st_mode)

    @renew_connection
    def isfile(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        return stat.S_ISREG(self._sftp.stat(path).st_mode)

    @renew_connection
    def getabs(self, path):
        """ get the absolute path

        Parameters
        ----------
        path: str

        Returns
        -------
        abspath: str

        """
        return os.path.join(self._sftp.getcwd(), path)

    @renew_connection
    def makedirs(self, path):
        """

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        parts = splitall(path)
        curdir = ""
        for part in parts:
            curdir = os.path.join(curdir, part)
            if not self.exists(curdir):
                self._sftp.mkdir(curdir)

    # TODO Passes tests but could probably be written better
    def _recursive_glob(self, path, pattern, file_list):

        if "**" in pattern:
            recursive = True
        else:
            recursive = False

        root = self._sftp.listdir(path)

        for f in (os.path.join(path, entry) for entry in root):
            f_stat = self._sftp.stat(f)
            if fnmatch(f, pattern):
                if pattern.endswith("**") and stat.S_ISDIR(f_stat.st_mode):
                    file_list.append(f)
                elif pattern.endswith("**") and stat.S_ISREG(f_stat.st_mode):
                    continue
                elif stat.S_ISDIR(f_stat.st_mode) and recursive:
                    pass
                else:
                    file_list.append(f)
                    continue
            if stat.S_ISDIR(f_stat.st_mode) and recursive:
                if f not in file_list:
                    file_list.append(f)
                self._recursive_glob(f, pattern, file_list)
                continue
            patternlist = splitall(pattern)
            nodblstar = os.path.join(*[p for p in patternlist if p != "**"])
            if not pattern.endswith("**") and fnmatch(f, nodblstar):
                file_list.append(f)
            pathlist = splitall(f)
            if stat.S_ISDIR(f_stat.st_mode) and len(patternlist) > len(pathlist):
                l = len(pathlist)
                if fnmatch(f, os.path.join(*patternlist[:l])):
                    self._recursive_glob(f, pattern, file_list)

    @renew_connection
    def glob(self, pattern):
        """

        Parameters
        ----------
        pattern: str

        Returns
        -------

        """
        if not pattern or pattern == ".":
            return
        if pattern.startswith(".."):
            raise IOError("cannot go outside folder context")
        patternlist = splitall(pattern)
        if patternlist[0] == ".":
            pattern = os.path.join(*patternlist[1:])

        file_list = []
        self._recursive_glob("", pattern, file_list)
        for path in file_list:
            yield path

    @renew_connection
    def rmtree(self, path):
        """remove all files and folders in path

        Parameters
        ----------
        path: str

        Returns
        -------

        """
        if path == "" or path == ".":
            raise IOError("attempting to remove the root directory")
        if not self.exists(path):
            raise IOError("root doesn't exist: {}".format(path))
        if not self.isdir(path):
            raise IOError("root is not a directory: {}".format(path))
        print(os.path.join(path, "**", "*"))
        print(list(reversed(sorted(self.glob(os.path.join(path, "**", "*"))))))
        for subpath in reversed(sorted(self.glob(os.path.join(path, "**/*")))):
            if self.isfile(subpath):
                self._sftp.remove(subpath)
            else:
                self._sftp.rmdir(subpath)

    @renew_connection
    def rename(self, path, newname):
        """

        Parameters
        ----------
        path: str
            relative to root
        newname: str

        Returns
        -------

        """
        self._sftp.rename(path, os.path.join(os.path.dirname(path), newname))

    @renew_connection
    def remove(self, path):
        """

        Parameters
        ----------
        path: str
            relative to root

        Returns
        -------

        """
        if self.isfile(path):
            self._sftp.remove(path)
        else:
            self._sftp.rmdir(path)

    # TODO can get callbacks while uploading
    @renew_connection
    def copy_from(self, source, path):
        """ copy from a local source outside the context folder

        Parameters
        ----------
        source: str or path_like
        path: str

        Returns
        -------

        """
        if not self.exists(path):
            raise IOError("path doesn't exist: {}".format(path))
        if isinstance(source, basestring):
            source = pathlib.Path(source)
        if not source.exists():
            raise IOError("source doesn't exist: {}".format(source))
        if source.is_file():
            with source.open() as file_obj:
                self._sftp.putfo(file_obj, os.path.join(path, source.name))
        else:
            self._sftp.mkdir(source.name)
            for subsource in source.iterdir():
                self.copy_from(subsource, os.path.join(path, source.name))

    # TODO can get callbacks while downloading
    @renew_connection
    def copy_to(self, path, target):
        """ copy to a local target outside the context folder

        Parameters
        ----------
        path: str
        target: str or path_like

        Returns
        -------

        """
        if not self.exists(path):
            raise IOError("path doesn't exist: {}".format(path))
        if isinstance(target, basestring):
            target = pathlib.Path(target)
        if not target.exists():
            raise IOError("target doesn't exist: {}".format(target))
        if path == "." or path == "":
            targetchild = target.joinpath(os.path.basename(self._sftp.getcwd()))
        else:
            targetchild = target.joinpath(os.path.basename(path))
        if self.isfile(path):
            targetchild.touch()
            print(targetchild, path)
            with targetchild.open("wb") as file_obj:
                self._sftp.getfo(path, file_obj)
        else:
            targetchild.mkdir()
            for childpath in self.glob(os.path.join(path, "*")):
                self.copy_to(childpath, targetchild)

    @renew_connection
    @contextmanager
    def open(self, path, mode='r', **kwargs):
        """

        Parameters
        ----------
        path: str
        mode: str

        Returns
        -------

        """
        # current version of paramiko has a bug returning bytes instead of text (paramiko/paramiko#403)
        with self._sftp.open(path, mode) as file_obj:
            if 'b' not in mode:
                yield codecs.getreader("utf-8")(file_obj)
            else:
                yield file_obj

    # TODO stream stdout
    @renew_connection
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
        if not self.exists(path):
            raise IOError("path doesn't exist: {}".format(path))

        if path and not path == ".":
            full_path = os.path.join(self._sftp.getcwd(), path)
        else:
            full_path = self._sftp.getcwd()
        if full_path and full_path is not None:
            cmnd = "cd {}; ".format(full_path) + cmnd

        stdin, stdout, stderr = self._ssh.exec_command(cmnd)
        error = stderr.read().decode()
        exitcode = stdout.channel.recv_exit_status()

        if exitcode:
            err_msg = "the following line caused error code {0}: {1}\n{2}".format(exitcode, cmnd, error)
            logging.error(err_msg)
            if raise_error:
                raise RuntimeError(err_msg)

    # TODO will only work for unix based systems
    # TODO overwriting?
    @renew_connection
    def copy(self, inpath, outpath):
        """

        Parameters
        ----------
        inpath: str
        outpath: str

        Returns
        -------

        """
        if not self.exists(inpath):
            raise IOError("inpath doesn't exist: {}".format(inpath))
        if not self.exists(outpath):
            raise IOError("outpath doesn't exist: {}".format(outpath))
        if not self.isdir(outpath):
            raise IOError("outpath is not a directory: {}".format(outpath))

        if self.isfile(inpath):
            self.exec_cmnd('cp -pR {0} {1}/'.format(inpath, outpath))
        else:
            self.makedirs(os.path.basename(inpath))
            self.exec_cmnd('cp -pR {0}/ {1}/{2}/'.format(inpath, outpath, os.path.basename(inpath)))


class change_dir(object):
    """a context manager for changing the current working directory"""

    def __init__(self, path='.', remote=False, hostname='', **kwargs):
        """

        Parameters
        ----------
        path: str, pathlib.Path or path_like
        remote: bool
            whether connecting to a remote server
        hostname: str
            if remote, the server host to connect to
        kwargs:
            additional keyword arguments for paramiko.client.SSHClient.connect

        """
        if path == "":
            path = "."

        if not remote:
            self._remote = False
            if isinstance(path, basestring):
                if not os.path.exists(path):
                    os.makedirs(path)
                    #raise IOError("the path does not exist: {}".format(path))
                if not os.path.isdir(path):
                    raise IOError("the path is not a directory: {}".format(path))
                abspath = os.path.expanduser(os.path.abspath(path))
                self._path = pathlib.Path(abspath)
            else:
                if not hasattr(path, "is_dir"):
                    raise IOError("path is not path_like: {}".format(path))
                if not path.exists():
                    path.mkdir()
                    #raise IOError("the path does not exist: {}".format(path))
                if not path.is_dir():
                    raise IOError("path is not a directory: {}".format(path))
                self._path = path  # .absolute()
        else:
            self._remote = True
            if not isinstance(path, basestring):
                raise IOError("the path must be a string: {}".format(path))
            self._path = path
            self._hostname = hostname
            self._kwargs = kwargs
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # try connecting
            self._ssh.connect(self._hostname, **self._kwargs)
            sftp = self._ssh.open_sftp()
            #sftp.chdir(self._path)
            self._ssh.close()

    def __enter__(self):
        if not self._remote:
            return LocalPath(self._path)
        else:
            return RemotePath(self._ssh, self._hostname, self._path, **self._kwargs)

    def __exit__(self, etype, value, traceback):
        try:
            self._ssh.close()
        except:
            pass
