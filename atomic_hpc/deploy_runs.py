"""
module to deploy runs
"""
import copy
import os
import logging
import re
# python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib
try:
    unicode
except NameError:
    unicode=str

from atomic_hpc import context_folder

_REGEX_VAR = r"(?:\@v\{[^}]+\})"
_REGEX_FILE = r"(?:\@f\{[^}]+\})"

logger = logging.getLogger(__name__)


def deploy_runs(top_level_runs, config_path, exists_error=False, exec_errors=False, separate_dir=False):
    """

    Parameters
    ----------
    top_level_runs: list
        top level runs
    config_path: str or path_like
        the path of the config file
    exists_error: bool
        if True, raise an IOError if the output path already exists
    exec_errors: bool
        if True, raise Error if exec commands return with errorcode
    separate_dir: bool
        for local:
        if True and parent_dir is not None, the run will take place in the parent_dir
        if False and parent_dir is not None, the contents of the parent_dir will be copied to the output folder

    Returns
    -------
    """
    for run in top_level_runs:
        if run["environment"] in ["unix", "windows"]:
            _deploy_run_normal(run, config_path, exists_error=exists_error, exec_errors=exec_errors, separate_dir=separate_dir)
        elif run["environment"] == "qsub":
            _deploy_run_qsub(run, config_path, exists_error=exists_error, exec_errors=exec_errors, separate_dir=separate_dir)
        else:
            raise ValueError("unknown environment: {}".format(run["environment"]))


# TODO script/file contents shouldn't all be stored in memory (but how to do consistently for local/remote?)
def _get_inputs(run, config_path):
    """ get the inputs

    Parameters
    ----------
    run: dict
        conforming to run schema
    config_path: str or path like
        path to configuration file

    Returns
    -------
    variables: dict
        {name: value}
    files: dict
        {filename, content_text}
    scripts: dict
        {scriptname: content_text}

    """

    if run["input"] is None:
        return {}, {}, {}
    if isinstance(config_path, basestring):
        config_path = pathlib.Path(config_path)
    if run["input"]["path"] is None:
        inpath = ""
    else:
        inpath = run["input"]["path"]
    if run["input"]["remote"] is None:
        kwargs = dict(path=config_path.joinpath(inpath))
    else:
        remote = run["input"]["remote"].copy()
        hostname = remote.pop("hostname")
        kwargs = dict(path=inpath, remote=True, hostname=hostname, **remote)

    with context_folder.change_dir(**kwargs) as folder:

        if run["input"]["variables"] is not None:
            variables = copy.copy(run["input"]["variables"])
        else:
            variables = {}

        files = {}
        if run["input"]["files"] is not None:
            for fid, fpath in run["input"]["files"].items():
                if not folder.exists(fpath):
                    raise ValueError("run {0}: files path does not exist: {1}".format(run["id"], fpath))
                if not folder.isfile(fpath):
                    raise ValueError("run {0}: files path is not a file: {1}".format(run["id"], fpath))
                with folder.open(fpath) as f:
                    files[fid] = (folder.name(fpath), f.read())

        scripts = {}
        if run["input"]["scripts"] is not None:
            for spath in run["input"]["scripts"]:

                # create main script
                if not folder.exists(spath):
                    raise ValueError("run {0}: script path does not exist: {1}".format(run["id"], spath))
                if not folder.isfile(spath):
                    raise ValueError("run {0}: files path is not a file: {1}".format(run["id"], spath))
                scriptname = folder.name(spath)

                if scriptname in scripts:
                    raise ValueError("run {0}: two scripts with same name: {1}".format(run["id"], scriptname))

                with folder.open(spath) as f:
                    script = f.read()

                # insert variables
                var_error = "run {id}: variable name; {var_name} not available to replace in script; {spath}"
                for tag in re.findall(_REGEX_VAR, script):
                    var = tag[3:-1]
                    if var not in variables:
                        raise KeyError(var_error.format(id=run["id"], var_name=var, spath=spath))
                    script = script.replace(tag, str(variables[var]))

                # insert file contents
                file_error = "run {id}: file; {path_name} not available to replace in script; {spath}"
                for tag in re.findall(_REGEX_FILE, script):
                    var = tag[3:-1]
                    if var not in files:
                        raise KeyError(file_error.format(id=run["id"], path_name=var, spath=spath))
                    script = script.replace(tag, files[var][1])

                scripts[scriptname] = script

    return variables, dict(files.values()), scripts


def _replace_in_cmnd(cmndline, variables, rid):
    """ replace variables in cmnd string

    Parameters
    ----------
    cmndline: str
    variables: dict or None
    rid: int

    Returns
    -------

    """
    var_error = "error in run {id}: variable name; {var_name} not available to replace in cmndline; {cmnd}"
    for tag in re.findall(_REGEX_VAR, cmndline):
        var = tag[3:-1]
        if variables is None:
            raise KeyError(var_error.format(id=rid, var_name=var, cmnd=cmndline))
        elif var not in variables:
            raise KeyError(var_error.format(id=rid, var_name=var, cmnd=cmndline))
        cmndline = cmndline.replace(tag, str(variables[var]))
    return cmndline


def _deploy_run_normal(run, root_path, exists_error=False, exec_errors=False, parent_dir=None, parent_kwargs=None, separate_dir=False):
    """ deploy run and child runs (recursively)

    Parameters
    ----------
    run: dict
        top level run
    root_path: str or path_like
        the path to resolve (local) relative paths from
    exists_error: bool
        if True, raise an IOError if the output path already exists
    exec_errors: bool
        if True, raise Error if exec commands return with errorcode
    parent_dir: str or None
        the parent output directory (if dependant on another run's output)
     parent_kwargs : dict
        the parent keyword arguments to connect to the output directory (if dependant on another run's output)

    Returns
    -------

    """
    logger.info("gathering inputs for run: {0}: {1}".format(run["id"], run["name"]))

    # get inputs
    variables, files, scripts = _get_inputs(run, root_path)

    # get commands
    environment = run["environment"]
    cmnds = [_replace_in_cmnd(cmnd, variables, run["id"]) for cmnd in run["process"][environment]["run"]]

    # open output
    if parent_kwargs is None:
        if isinstance(root_path, basestring):
            root_path = pathlib.Path(root_path)
        if run["output"]["path"] is None:
            outpath = ""
        else:
            outpath = run["output"]["path"]
        if run["output"]["remote"] is None:
            logger.info("running locally: {0}: {1}".format(run["id"], run["name"]))
            kwargs = dict(path=root_path.joinpath(outpath))
        else:
            logger.info("running remotely: {0}: {1}".format(run["id"], run["name"]))
            remote = run["output"]["remote"].copy()
            hostname = remote.pop("hostname")
            kwargs = dict(path=outpath, remote=True, hostname=hostname, **remote)
    else:
        logger.info("running child: {0}: {1}".format(run["id"], run["name"]))
        kwargs = parent_kwargs

    with context_folder.change_dir(**kwargs) as folder:

        logger.info("executing run: {0}: {1}".format(run["id"], run["name"]))

        # create output folder
        if parent_dir is None or separate_dir:
            outdir = "{0}_{1}".format(run["id"], run["name"])
            if folder.exists(outdir):
                if exists_error:
                    logger.critical("aborting run: output dir already exists: {}".format(outdir))
                    return False
                logger.info("removing existing output dir: {}".format(outdir))
                folder.rmtree(outdir)
            folder.makedirs(outdir)
            if separate_dir and parent_dir is not None:
                for ppath in folder.glob(os.path.join(parent_dir, "*")):
                    folder.copy(ppath, outdir)
        else:
            outdir = parent_dir

        for fname, fcontent in files.items():
            with folder.open(os.path.join(outdir, fname), 'w') as f:
                f.write(fcontent)
        for sname, scontent in scripts.items():
            with folder.open(os.path.join(outdir, sname), 'w') as f:
                f.write(scontent)

        # run commands
        for cmndline in cmnds:
            logging.info("executing: {}".format(cmndline))
            try:
                folder.exec_cmnd(cmndline, outdir, raise_error=True)
            except RuntimeError:
                if exec_errors:
                    logger.critical("aborting run on command line failure: {}".format(cmndline))
                    return False
                logger.error("command line failure: {}".format(cmndline))

            logging.info("finished execution")

        logger.info("finalising run: {0}: {1}".format(run["id"], run["name"]))

        # cleanup output
        if run["output"]["remove"] is not None:
            for path in run["output"]["remove"]:
                rmpath = os.path.join(outdir, path)
                if folder.exists(rmpath):
                    logger.debug("removing {0} from output".format(rmpath))
                    if folder.isdir(rmpath):
                        folder.rmtree(rmpath)
                    else:
                        folder.remove(rmpath)
                for gpath in folder.glob("*{}".format(rmpath)):
                    logger.debug("removing {0} from output".format(gpath))
                    folder.remove(gpath)

        if run["output"]["rename"] is not None:
            for old, new in run["output"]["rename"].items():
                renamedir = os.path.join(outdir, "*{}".format(old))
                for path in folder.glob(renamedir):
                    newname = os.path.basename(path)[:-len(old)] + new
                    logger.debug("renaming {0} to {1}".format(path, newname))
                    folder.rename(path, newname)

    for child_run in run["children"]:
        _deploy_run_normal(child_run, root_path, exists_error=exists_error, exec_errors=exec_errors,
                           parent_dir=outdir, parent_kwargs=kwargs, separate_dir=separate_dir)


_qsub_top_template = """#!/bin/bash --login
#PBS -N {jobname:.14}
#PBS -l walltime={walltime}
#PBS -l select={nnodes}:ncpus={ncores}
#PBS -j oe
{pbs_optional}

echo "<qstat -f $PBS_JOBID>"
qstat -f $PBS_JOBID
echo "</qstat -f $PBS_JOBID>"

# number of cores per node used
export NCORES={ncores}
# number of processes
export NPROCESSES={nprocs}

# Make sure any symbolic links are resolved to absolute path
export PBS_O_WORKDIR=$(readlink -f $PBS_O_WORKDIR)

# Set the number of threads to 1
#   This prevents any system libraries from automatically 
#   using threading.
export OMP_NUM_THREADS=1

{load_modules}

# commands to run before main run (in $WORKDIR)
{exec_before_run}

# copy required input files from $WORKDIR to $TMPDIR
{copy_to_temp}

# main commands to run (in $TMPDIR)
{exec_run}

# copy required output files from $TMPDIR to $WORKDIR
{copy_from_temp}

# commands to run after main run (in $WORKDIR)
{exec_after_run}

"""


def _resolve_walltime(walltime):
    """ ensure wall time is in format HH:MM:SS

    Parameters
    ----------
    walltime: str

    Returns
    -------

    """
    components = walltime.split(":")
    for c in components:
        try:
            int(c)
        except ValueError:
            raise ValueError("the walltime is not in the correct format: {}".format(walltime))

    if len(components) > 3:
        raise ValueError("the walltime is not in the correct format: {}".format(walltime))
    elif len(components) == 3:
        return ':'.join([components[0], '{:02d}'.format(int(components[1])), '{:02d}'.format(int(components[2]))])
    elif len(components) == 2:
        return ':'.join([components[0], '{:02d}'.format(int(components[1])), "00"])
    elif len(components) == 1:
        return ':'.join([components[0], "00", "00"])


def _create_qsub(qsub, wrkpath, fnames, execruns, copyregex):
    """

    Parameters
    ----------
    qsub: dict
    wrkpath: str
        absolute path of working directory
    fnames: list of str
        input file names
    execruns: list of str
        commands to run
    copyregex: list of str
        regexes to specify files to copy back to working directory


    Returns
    -------
    qsub: str
        contents of qsub file

    """

    # get qsub options
    jobname = qsub["jobname"] if qsub["jobname"] is not None else "unknown"
    walltime = _resolve_walltime(qsub["walltime"])
    nnodes = qsub["nnodes"]
    ncores = qsub["cores_per_node"]
    nprocs = nnodes * ncores
    pbs_optional = ""
    pbs_optional += "#PBS -q \n" + qsub["queue"] if qsub["queue"] is not None else ""
    # Sends email to the submitter when the job begins/ends/aborts
    if qsub.get("email", None) is not None:
        pbs_optional += "#PBS -M {}\n".format(qsub["email"])
        pbs_optional += "#PBS -m bae\n"

    # get modules to load
    load_modules = "module load " + " ".join(qsub["modules"]) if qsub["modules"] is not None else ""

    # commands to run before run (in $WORKDIR)
    exec_before_run = "" # TODO

    # copy to temp
    copy_to_temp = ""
    for name in fnames:
        copy_to_temp += "cp -p {0} $TMPDIR\n".format(os.path.join(wrkpath, name))

    # exec runs (in $TMPDIR)
    exec_run = "\n".join(execruns)

    # copy from temp
    copy_from_temp = ""
    for regex in copyregex:
        copy_from_temp += "cp -pR $TMPDIR/{0} {1} \n".format(regex, wrkpath)

    # commands to run after run (in $WORKDIR)
    exec_after_run = "" # TODO

    out = _qsub_top_template.format(jobname=jobname, walltime=walltime, nnodes=nnodes,
                                    ncores=ncores, nprocs=nprocs, pbs_optional=pbs_optional, load_modules=load_modules,
                                    exec_before_run=exec_before_run, copy_to_temp=copy_to_temp,
                                    exec_run=exec_run, copy_from_temp=copy_from_temp, exec_after_run=exec_after_run)

    return out


# TODO children
def _deploy_run_qsub(run, root_path, exists_error=False, exec_errors=False, parent_dir=None, separate_dir=False):
    """ deploy run and child runs (recursively)

    Parameters
    ----------
    run: dict
        top level run
    root_path: str or path_like
        the path to resolve (local) relative paths from
    exists_error: bool
        if True, raise an IOError if the output path already exists
    exec_errors: bool
        if True, raise Error if exec commands return with errorcode
    parent_dir: str or None
        the parent output directory (if dependant on another run's output)
     # parent_kwargs : dict
     #    the parent keyword arguments to connect to the output directory (if dependant on another run's output)

    Returns
    -------

    """
    logger.info("gathering inputs for qsub run: {0}: {1}".format(run["id"], run["name"]))

    # get inputs
    variables, files, scripts = _get_inputs(run, root_path)
    fnames = list(scripts.keys()) + list(files.keys())

    # get commands
    # cmnds_before = [_replace_in_cmnd(cmnd, variables, run["id"]) for cmnd in run["process"]["qsub"]["before_run"]]
    cmnds_run = [_replace_in_cmnd(cmnd, variables, run["id"]) for cmnd in run["process"]["qsub"]["run"]]
    # cmnds_after = [_replace_in_cmnd(cmnd, variables, run["id"]) for cmnd in run["process"]["qsub"]["after_run"]]

    # get from_temp
    copyregex = []
    for regex in run["process"]["qsub"]["from_temp"]:
        if regex.startswith("*"):
            copyregex.append(regex)
        else:
            copyregex.append("*"+regex)

    # open output
    if isinstance(root_path, basestring):
        root_path = pathlib.Path(root_path)
    if run["output"]["path"] is None:
        outpath = ""
    else:
        outpath = run["output"]["path"]
    if run["output"]["remote"] is None:
        logger.info("running locally: {0}: {1}".format(run["id"], run["name"]))
        kwargs = dict(path=root_path.joinpath(outpath))
    else:
        logger.info("running remotely: {0}: {1}".format(run["id"], run["name"]))
        remote = run["output"]["remote"].copy()
        hostname = remote.pop("hostname")
        kwargs = dict(path=outpath, remote=True, hostname=hostname, **remote)

    with context_folder.change_dir(**kwargs) as folder:

        logger.info("executing qsub run: {0}: {1}".format(run["id"], run["name"]))

        # create output folder
        outdir = "{0}_{1}".format(run["id"], run["name"])
        if folder.exists(outdir):
            if exists_error:
                logger.critical("aborting run: output dir already exists: {}".format(outdir))
                return False
            logger.info("removing existing output dir: {}".format(outdir))
            folder.rmtree(outdir)
        folder.makedirs(outdir)

        for fname, fcontent in files.items():
            with folder.open(os.path.join(outdir, fname), 'w') as f:
                f.write(fcontent)
        for sname, scontent in scripts.items():
            with folder.open(os.path.join(outdir, sname), 'w') as f:
                f.write(scontent)

        # make qsub
        if run["process"]["qsub"]["jobname"] is None:
            run["process"]["qsub"]["jobname"] = '{0}_{1}'.format(run["id"], run["name"])

        abspath = folder.getabs(outdir)
        qsub = _create_qsub(run["process"]["qsub"], abspath, fnames, cmnds_run, copyregex)

        with folder.open(os.path.join(outdir, "run.qsub"), 'w') as f:
            f.write(unicode(qsub))

        # run
        # TODO should do source loading more flexibly
        # TODO something is not loaded to get emails
        cmndline = "source /etc/bashrc; source /etc/profile; qsub run.qsub"
        try:
            folder.exec_cmnd(cmndline, outdir, raise_error=True)
        except RuntimeError:
            if exec_errors:
                logger.critical("aborting run on command line failure: {}".format(cmndline))
                return False
            logger.error("command line failure: {}".format(cmndline))
