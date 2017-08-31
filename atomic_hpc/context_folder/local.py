import os
import shutil
from subprocess import Popen, PIPE, STDOUT
from contextlib import contextmanager
import logging
from threading import Thread
from atomic_hpc.context_folder.abstract import VirtualDir
from atomic_hpc.utils import splitall

# python 3 to 2 compatibility
try:
    basestring
except NameError:
    basestring = str
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib
try:
    from queue import Queue
except:
    from Queue import Queue

logger = logging.getLogger(__name__)


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
        logger.debug("making directories: {}".format(path))

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
        logger.debug("removing directories: {}".format(path))

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
        logger.debug("removing path: {}".format(path))
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
        logger.debug("removing path: {0} to {1}".format(path, newname))
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
        logger.debug("internally copying {0} to {1}".format(inpath, outpath))

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
        logger.debug("copying external source {0} to {1}".format(source, path))

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
        logger.debug("copying {0} to external target to {1}".format(path, target))

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

    # def exec_cmnd(self, cmnd, path='.', raise_error=False, timeout=None):
    #     """ perform a command line execution
    #
    #     Parameters
    #     ----------
    #     cmnd: str
    #     path: str
    #     raise_error: True
    #         raise error if a non zero exit code is received
    #     timeout: None or float
    #         seconds to wait for a pending read/write operation before raising an error
    #
    #     Returns
    #     -------
    #     success: bool
    #
    #     """
    #     logger.debug("executing command in {0}: {1}".format(path, cmnd))
    #
    #     runpath = self._root.joinpath(path)
    #     runpath.absolute()
    #     with self._exec_in_dir(runpath):
    #         # subprocess.run(cmnd, shell=True, check=True)
    #         process = Popen(cmnd, stdout=PIPE, stderr=PIPE, shell=True)
    #         with process.stdout as pipe:
    #             for line in iter(pipe.readline, b''):
    #                 logger.info('{}'.format(line.decode("utf-8").strip()))
    #         with process.stderr as errpipe:
    #             for line in iter(errpipe.readline, b''):
    #                 logger.warning('{}'.format(line.decode("utf-8").strip()))
    #
    #         exitcode = process.wait(timeout=timeout)  # 0 means success
    #         if exitcode:
    #             err_msg = "the following line in caused error code {0}: {1}".format(exitcode, cmnd)
    #             logger.error(err_msg)
    #             if raise_error:
    #                 raise RuntimeError(err_msg)
    #             logging.error(err_msg)
    #             return False
    #
    #     logger.debug("successfully executed command in {0}: {1}".format(path, cmnd))
    #     return True

    @staticmethod
    def _pipe_reader(pipe, name, queue):
        try:
            with pipe:
                for line in iter(pipe.readline, b''):
                    queue.put((pipe, name, line))
        finally:
            queue.put(None)

    # TODO timeout doesn't work in wait
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

        Notes
        -----
        queuing allows stdout and stderr to output as separate streams, but in (almost) the right order
        based on: https://stackoverflow.com/a/31867499/5033292

        """
        logger.debug("executing command in {0}: {1}".format(path, cmnd))

        security = self.check_cmndline_security(cmnd)
        if security is not None:
            if raise_error:
                raise RuntimeError(security)
            logging.error(security)
            return False

        runpath = self._root.joinpath(path)
        runpath.absolute()
        with self._exec_in_dir(runpath):
            # subprocess.run(cmnd, shell=True, check=True)
            process = Popen(cmnd, stdout=PIPE, stderr=PIPE, shell=True, bufsize=1)
            q = Queue()
            Thread(target=self._pipe_reader, args=[process.stdout, "out", q]).start()
            Thread(target=self._pipe_reader, args=[process.stderr, "error", q]).start()

            for _ in range(2):
                for source, name, line in iter(q.get, None):
                    if name == "out":
                        logger.info(line.decode("utf-8").strip())
                    elif name == "error":
                        logger.warning(line.decode("utf-8").strip())
                    else:
                        raise ValueError("somethings gone wrong")

            exitcode = process.wait()  # 0 means success
            if exitcode:
                err_msg = "the following line in caused error code {0}: {1}".format(exitcode, cmnd)
                logger.error(err_msg)
                if raise_error:
                    raise RuntimeError(err_msg)
                logging.error(err_msg)
                return False

        logger.debug("successfully executed command in {0}: {1}".format(path, cmnd))
        return True
