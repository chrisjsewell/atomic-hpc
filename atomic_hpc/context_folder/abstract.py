import os
from contextlib import contextmanager
from fnmatch import fnmatch


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
        raise NotImplementedError

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
        raise NotImplementedError

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

    def iterdir(self, path=""):
        """

        Yields
        -------
        subpath:
            each subpath in the folder

        """
        for path in self.glob(os.path.join(path, "*")):
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

    def copy_from(self, source, path):
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


    # TODO improve command line security: https://security.openstack.org/guidelines/dg_avoid-shell-true.html,
    # https://docs.python.org/3/library/shlex.html#shlex.quote
    @staticmethod
    def check_cmndline_security(line):
        """ some basic security checks for the command line to be run

        Parameters
        ----------
        line

        Returns
        -------

        Notes
        -----
        https://www.tecmint.com/10-most-dangerous-commands-you-should-never-execute-on-linux/

        """
        security_risks = [
            "rm -rf / ",
            "rm -rf /;",
            ":(){:|:&};:",
            " > /dev/sda",
            " > /dev/hda",
            "mv * /dev/null",
            "mkfs.ext3 /dev/sda",
            "mkfs.ext3 /dev/hda",
            "dd if=/dev/random of=/dev/sda",
            "dd if=/dev/zero of=/dev/hda",
            "dd if=/dev/zero of=/dev/sda",
            "mv / /dev/null",
            "dd if=/dev/random of=/dev/port",
            "echo 1 > /proc/sys/kernel/panic",
            "cat /dev/port",
            "cat /dev/zero > /dev/mem",
            "wget * -O- | sh",
            "rm -f /usr/bin/sudo",
            "rm -f /bin/su",
        ]
        for srisk in security_risks:
            if fnmatch(line, "*{}*".format(srisk)):
                return "command line contains security risk: {}".format(srisk)

        return None

    def exec_cmnd(self, cmnd, path, raise_error=False, timeout=None):
        """

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
        raise NotImplementedError
