import logging
import os
from fnmatch import fnmatch
import sys

try:
    from builtins import input
except ImportError:
    input = raw_input
try:
    from distutils.util import strtobool
except ImportError:
    from distutils import strtobool


def splitall(path):
    """ split a path into a list of its components

    Parameters
    ----------
    path: str

    Returns
    -------
    pathlist: list of str

    """
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


def fnmatch_path(path, pattern, isafile=False):
    """ match a path, with a pattern which can contain wildcards and ** recursion

    Parameters
    ----------
    path: str
    pattern: str
    isafile: bool

    Returns
    -------
    match: bool

    """

    if not pattern:
        raise ValueError("Unacceptable pattern: {}".format(pattern))

    pathlist = splitall(path)
    patternlist = splitall(pattern)

    if patternlist[-1] == "**" and isafile:
        return False

    if patternlist[0] == ".":
        patternlist = patternlist[1:]

    if not patternlist:
        raise ValueError("Unacceptable pattern: {}".format(pattern))

    # count **
    dblstars = 0
    dblstars_index = None
    for i, patt in enumerate(patternlist):
        if "**" in patt:
            if len(patt) > 2:
                raise ValueError("** must be a separate component: {}".format(pattern))
            else:
                dblstars += 1
                dblstars_index = i

    if not dblstars:
        if len(pathlist) != len(patternlist):
            return False
        for comp, patt in zip(pathlist, patternlist):
            if not fnmatch(comp, patt):
                return False
    elif dblstars > 1:
        raise NotImplementedError("only allowed one ** per regex: {}".format(pattern))
    else:
        if len(pathlist) < len(patternlist) - 1:
            return False
        # match from front upto **
        for comp, patt in zip(pathlist[:dblstars_index], patternlist[:dblstars_index]):
            if not fnmatch(comp, patt):
                return False
        # match from back (in reverse) to **
        dblstars_rev = len(patternlist) - dblstars_index - 1
        for comp, patt in zip(pathlist[-dblstars_rev:], patternlist[-dblstars_rev:]):
            if not fnmatch(comp, patt):
                return False

    return True


def glob_path(path, pattern, walk_func=os.walk):
    """ match a path, with a pattern which can contain wildcards and ** recursion

    Parameters
    ----------
    path: object
    pattern: str
    walk_func: func
        function which takes `path` as an argument and yields (dirpath, dirnames, filenames)

    Returns
    -------
    paths: list of str

    """
    for root, dirs, files in walk_func(path):

        for basename in files:
            filepath = os.path.join(root, basename)
            if fnmatch_path(filepath, pattern, True):
                yield filepath

        for basename in dirs:
            folderpath = os.path.join(root, basename)
            if fnmatch_path(folderpath, pattern, False):
                yield folderpath


def walk_path(path, listdir=os.listdir,
              isfile=os.path.isfile, isfolder=os.path.isdir):
    """

    Parameters
    ----------
    path: str
    listdir: func
        return list of subpath names in `path`
    isfile: func
    isfolder: func

    Returns
    -------

    """
    dirnames = []
    filenames = []

    for subpathname in listdir(path):
        subpath = os.path.join(path, subpathname)
        if isfile(subpath):
            filenames.append(subpathname)
        if isfolder(subpath):
            dirnames.append(subpathname)

    yield path, dirnames, filenames

    for dname in dirnames:
        subpath = os.path.join(path, dname)
        for newpath, dirnames, filenames in walk_path(subpath, listdir, isfile, isfolder):
            yield newpath, dirnames, filenames


def add_loglevel(name, levelnum, methodname=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    Adapted from: https://stackoverflow.com/a/35804945/5033292

    Parameters
    -----------
    name: str
        becomes an attribute of the `logging` module with the value
        `levelnum`. `methodname` becomes a convenience method for both `logging`
        itself and the class returned by `logging.getLoggerClass()` (usually just
        `logging.Logger`). If `methodname` is not specified, `name.lower()` is used.
    levelnum: int
    methodname: str or None

    Notes
    -----
    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> add_loglevel('TRACE',logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not methodname:
        methodname = name.lower()

    if hasattr(logging, name):
        raise AttributeError('{} already defined in logging module'.format(name))
    if hasattr(logging, methodname):
        raise AttributeError('{} already defined in logging module'.format(methodname))
    if hasattr(logging.getLoggerClass(), methodname):
        raise AttributeError('{} already defined in logger class'.format(methodname))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelnum):
            self._log(levelnum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        if logging.root.isEnabledFor(levelnum):
            logging.root._log(levelnum, message, args, **kwargs)

    logging.addLevelName(levelnum, name)
    setattr(logging, name, levelnum)
    setattr(logging.getLoggerClass(), methodname, logForLevel)
    setattr(logging, methodname, logToRoot)


def cmndline_prompt(query):
    """ get a prompt from the user

    Parameters
    ----------
    query: str

    Returns
    -------

    """
    val = input("{0} [y/n]: ".format(query))
    try:
        ret = strtobool(val)
    except ValueError:
        sys.stdout.write('Please answer with a y/n\n')
        return cmndline_prompt(query)
    return ret


def str2intlist(s, delim=","):
    """ create a list of ints from a delimited string

    Parameters
    ----------
    s: string
    delim: string

    Returns
    -------
    int_list: list of ints

    Examples
    --------
    >>> str2intlist("1,2,3")
    [1, 2, 3]
    >>> str2intlist("1-3")
    [1, 2, 3]
    >>> str2intlist("2,3-4,6")
    [2, 3, 4, 6]

    >>> str2intlist("a")
    Traceback (most recent call last):
     ...
    TypeError: not a valid list of ints: "a"

    """
    def get_int(n):
        try:
            return int(n)
        except:
            raise TypeError('not a valid list of ints: "{}"'.format(s))

    return sum(((list(range(*[get_int(j) + k for k, j in enumerate(i.split('-'))]))
                 if '-' in i else [get_int(i)]) for i in s.split(delim)), [])
