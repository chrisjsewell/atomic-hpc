import os
from fnmatch import fnmatch


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
        elif parts[1] == path: # sentinel for relative paths
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
        if len(pathlist) < len(patternlist)-1:
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