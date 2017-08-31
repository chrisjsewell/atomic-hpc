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


def fnmatch_path(path, pattern, isafile):
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

    i = 0
    for pathcomp in pathlist:

        # if the path is longer than the pattern
        if i > len(patternlist)-1:
            return False

        if not fnmatch(pathcomp, patternlist[i]):
            return False

        # pause moving to next part of the pattern while **
        if not patternlist[i] == "**":
            i += 1
        # but go again if the next part of the pattern matches
        elif not i+1 > len(patternlist) - 1:
            if fnmatch(pathcomp, patternlist[i+1]):
                if not (i+2==len(patternlist) and patternlist[i+1]=="*"):
                    i += 1

    # fail if still part of the pattern left
    if not i > len(patternlist) - 1:
        # but not if the final part is ** or **/[match]
        if patternlist[-1] == "**":
            return True
        if len(patternlist) > 1:
            if patternlist[-2] == "**" and fnmatch(pathcomp, patternlist[-1]):
                return True
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