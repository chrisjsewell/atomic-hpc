import os
from contextlib import contextmanager


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

    def exec_cmnd(self, path, cmnd, raise_error=False, timeout=None):
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
