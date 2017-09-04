import stat
import os
import sys
import codecs
import select
from contextlib import contextmanager
try:
    basestring
except NameError:
    basestring = str
# python 3 to 2 compatibility
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

import logging
logger = logging.getLogger(__name__)

from atomic_hpc.context_folder.abstract import VirtualDir
from atomic_hpc.utils import walk_path, glob_path, splitall


# for writing binary output to stdout on windows
if sys.platform == "win32" or sys.platform == "win64":
    import msvcrt
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)


def renew_connection(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self._ssh.get_transport().is_active():
            logger.debug("renewing connection to remote host")
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
        # if "allow_agent" not in kwargs:
        #     self._kwargs["allow_agent"] = False
        # if "look_for_keys" not in kwargs:
        #     self._kwargs["look_for_keys"] = False
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
    def stat(self, path):
        """ Retrieve information about a file

        Parameters
        ----------
        path: str

        Returns
        -------
        attr: object
            see os.stat, includes st_mode, st_size, st_uid, st_gid, st_atime, and st_mtime attributes

        """
        return self._sftp.stat(path)

    @renew_connection
    def chmod(self, path, mode):
        """ Change the mode (permissions) of a file

        Parameters
        ----------
        path: str
        mode: int
            new permissions (see os.chmod)

        Examples
        --------
        To make a file executable
        cur_mode = folder.stat("exec.sh").st_mode
        folder.chmod("exec.sh", cur_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH )

        """
        return self._sftp.chmod(path, mode)

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
        logger.debug("making directories: {}".format(path))

        parts = splitall(path)
        curdir = ""
        for part in parts:
            curdir = os.path.join(curdir, part)
            if not self.exists(curdir):
                logger.debug("making sub-directory: {}".format(curdir))
                self._sftp.mkdir(curdir)

    @renew_connection
    def glob(self, pattern):
        """

        Parameters
        ----------
        pattern: str

        Returns
        -------

        """
        if pattern.startswith(".."):
            raise IOError("cannot go outside folder context")

        def walk_func(apath):
            return walk_path(apath, listdir=self._sftp.listdir, isfile=self.isfile, isfolder=self.isdir)

        for path in glob_path("", pattern, walk_func):
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
        logger.debug("removing directories: {}".format(path))

        if path == "" or path == ".":
            raise IOError("attempting to remove the root directory")
        if not self.exists(path):
            raise IOError("root doesn't exist: {}".format(path))
        if not self.isdir(path):
            raise IOError("root is not a directory: {}".format(path))
        print(os.path.join(path, "**", "*"))
        logger.debug("removing: {0}".format(list(self.glob(os.path.join(path, "**", "*")))))
        for subpath in reversed(sorted(self.glob(os.path.join(path, "**", "*")))):
            self.remove(subpath)
        if self.exists(path):
            self.remove(path)

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
        logger.debug("removing path: {0} to {1}".format(path, newname))
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
        logger.debug("removing path: {}".format(path))

        if self.isfile(path):
            try:
                self._sftp.remove(path)
            except IOError as err:
                raise IOError("failed to remove file; {0}, with error:\n{1}".format(path, err))
        else:
            if list(self.iterdir(path)):
                raise IOError("the folder; {0}, contains content, use rmtree if you wish to delete it".format(path))
            try:
                self._sftp.rmdir(path)
            except IOError as err:
                raise IOError("failed to remove folder; {0}, with error:\n{1}".format(path, err))

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
        logger.debug("copying external source {0} to {1}".format(source, path))
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
        logger.debug("copying {0} to external target to {1}".format(path, target))

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
            with targetchild.open("wb") as file_obj:
                self._sftp.getfo(path, file_obj)
        else:
            targetchild.mkdir()
            for childpath in self.glob(os.path.join(path, "*")):
                self.copy_to(childpath, targetchild)

    @renew_connection
    @contextmanager
    def open(self, path, mode='r', encoding=None):
        """

        Parameters
        ----------
        path: str
        mode: str
        encoding: None or str

        Returns
        -------

        """
        logger.debug("opening {0} in mode '{1}'".format(path, mode))
        # current version of paramiko has a bug returning bytes instead of text (paramiko/paramiko#403)
        with self._sftp.open(path, mode) as file_obj:
            if 'b' not in mode:
                yield codecs.getreader("utf-8")(file_obj)
            else:
                yield file_obj

    @staticmethod
    def _stream_exec(ssh, cmd, timeout,
                     stdout_func=None, stderr_func=None):
        """ stream the stdout and stderror to a function

        adapted from: https://github.com/paramiko/paramiko/issues/593
        and discussed at:

        Parameters
        ----------
        ssh: paramiko.client.SSHClient
        cmd: str
        timeout: None or float
        stdout_func: func
            must take input as bytes, defaults to sys.stdout
        stderr_func: func
            must take input as bytes, defaults to sys.stderr

        Returns
        -------
        exitcode:

        """
        if stdout_func is None:
            if hasattr(sys.stdout, "buffer"):
                stdout_func = sys.stdout.buffer.write
            else:
                stdout_func = sys.stdout.write
        if stderr_func is None:
            if hasattr(sys.stderr, "buffer"):
                stderr_func = sys.stderr.buffer.write
            else:
                stderr_func = sys.stderr.write

        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
        channel = stdout.channel

        # we do not need stdin.
        stdin.close()
        # indicate that we're not going to write to that channel anymore
        channel.shutdown_write()

        stdout_func(stdout.channel.recv(len(stdout.channel.in_buffer)))  # send to out_func

        while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
            got_chunk = False
            readq, _, _ = select.select([stdout.channel], [], [], 5.0)
            for c in readq:
                if c.recv_stderr_ready():
                    stderr_func(stderr.channel.recv_stderr(len(c.in_stderr_buffer)))  # send stderr to out_func
                    got_chunk = True
                if c.recv_ready():
                    stdout_func(stdout.channel.recv(len(c.in_buffer)))  # send to out_func
                    got_chunk = True
            if not got_chunk and channel.exit_status_ready() and not channel.recv_stderr_ready() and not channel.recv_ready():
                # indicate that we're not going to read from this channel anymore
                channel.shutdown_read()
                # close the channel
                channel.close()  # exit as remote side is finished and our bufferes are empty
                break
        stdout.close()
        stderr.close()
        return channel.recv_exit_status()

    @staticmethod
    def _log_output(pipe):
        for line in iter(pipe.decode("utf-8").strip().splitlines()):
            logger.info(line)


    @staticmethod
    def _log_error(pipe):
        for line in iter(pipe.decode("utf-8").strip().splitlines()):
            logger.warning(line)

    @renew_connection
    def exec_cmnd(self, cmnd, path='.', raise_error=False, timeout=None):
        """ perform a command line execution

        Parameters
        ----------
        cmnd: str
        path: str
        raise_error: True
            raise error if a non zero exit code is received
        timeout: None or float
            seconds to wait for a pending read/write operation before raising an error

        Returns
        -------
        success: bool

        """
        logger.debug("executing command in {0}: {1}".format(path, cmnd))

        security = self.check_cmndline_security(cmnd)
        if security is not None:
            if raise_error:
                raise RuntimeError(security)
            logging.error(security)
            return False

        if not self.exists(path):
            raise IOError("path doesn't exist: {}".format(path))

        if path and not path == ".":
            full_path = os.path.join(self._sftp.getcwd(), path)
        else:
            full_path = self._sftp.getcwd()
        if full_path and full_path is not None:
            cmnd = "cd {}; ".format(full_path) + cmnd

        # stdin, stdout, stderr = self._ssh.exec_command(cmnd)
        # exitcode = stdout.channel.recv_exit_status()
        exitcode = self._stream_exec(self._ssh, cmnd, timeout,
                                     stderr_func=self._log_error, stdout_func=self._log_output)

        if exitcode:
            err_msg = "the following line caused error code {0}: {1}\n".format(exitcode, cmnd)
            logger.error(err_msg)
            if raise_error:
                raise RuntimeError(err_msg)
            else:
                logging.error(err_msg)
                return False

        logger.debug("successfully executed command in {0}: {1}".format(path, cmnd))
        return True

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
        logger.debug("internally copying {0} to {1}".format(inpath, outpath))

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

