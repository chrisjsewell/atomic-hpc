""" a module to create a context manager for opening a
folder, that can be either local, virtual or remote

"""
import os
import logging
import paramiko
from atomic_hpc.context_folder.local import LocalPath
from atomic_hpc.context_folder.remote import RemotePath

try:
    basestring
except NameError:
    basestring = str
# python 3 to 2 compatibility
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

logger = logging.getLogger(__name__)


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
            try:
                self._ssh.connect(self._hostname, **self._kwargs)
                _ = self._ssh.open_sftp()
                #sftp.chdir(self._path)
                self._ssh.close()
            except Exception as err:
                raise RuntimeError("failed connecting to {0} with args: {1}\n{2}".format(
                    self._hostname, self._kwargs, err))

    def __enter__(self):

        if not self._remote:
            logger.debug("entering local path")
            return LocalPath(self._path)
        else:
            logger.debug("entering remote path")
            return RemotePath(self._ssh, self._hostname, self._path, **self._kwargs)

    def __exit__(self, etype, value, traceback):
        logger.debug("exiting path")
        try:
            self._ssh.close()
        except:
            pass
