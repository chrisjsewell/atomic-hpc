"""
module to deploy runs
"""
import copy
import os
import logging
import re
from collections import OrderedDict
# python 2/3 compatibility
from ruamel.yaml import YAML

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


# TODO script/file contents shouldn't all be stored in memory (but how to do consistently for local/remote?)
def get_inputs(run, config_path):
    """ get the inputs and resolve regex insertions

    Parameters
    ----------
    run: dict
        conforming to run schema
    config_path: str or path like
        path to configuration file

    Returns
    -------
    inputs: dct
        with keys:
            files: dict
                {filename, content_text}
            scripts: dict
                {scriptname: content_text}
            cmnds: list

    """
    files = {}
    scripts = {}
    variables = {}

    if run["input"] is not None:

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

            if run["input"]["files"] is not None:
                for fid, fpath in run["input"]["files"].items():
                    if not folder.exists(fpath):
                        raise ValueError("run {0}: files path does not exist: {1}".format(run["id"], fpath))
                    if not folder.isfile(fpath):
                        raise ValueError("run {0}: files path is not a file: {1}".format(run["id"], fpath))
                    with folder.open(fpath) as f:
                        files[fid] = (folder.name(fpath), f.read())

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

    # get commands
    environment = run["environment"]
    cmnds = []
    if run["process"][environment]["run"] is not None:
        cmnds = [_replace_in_cmnd(cmnd, variables, run["id"]) for cmnd in run["process"][environment]["run"]]

    return {"files": dict(files.values()), "scripts": scripts, "cmnds": cmnds}


# TODO tests for deploy_runs (top-level)
def deploy_runs(runs, root_path, exists_error=False, exec_errors=False):
    """

    Parameters
    ----------
    runs: list
        runs
    root_path: str or path_like
        the path of the config file
    exists_error: bool
        if True, raise an IOError if the output path already exists
    exec_errors: bool
        if True, raise Error if exec commands return with errorcode

    Returns
    -------
    """
    for run in runs:

        logger.info("gathering inputs for run: {0}: {1}".format(run["id"], run["name"]))

        # get inputs
        inputs = get_inputs(run, root_path)
        fnames = list(inputs["scripts"].keys())
        fnames += list(inputs["files"].keys())
        if not len(set(fnames)) == len(fnames):
            logging.critical("aborting run: there is a script or file name clash in the inputs: {}".format(fnames))
            continue

        if run["environment"] in ["unix", "windows"]:
            _deploy_run_normal(run, inputs, root_path, exists_error=exists_error, exec_errors=exec_errors)
        elif run["environment"] == "qsub":
            _deploy_run_qsub(run, inputs, root_path, exists_error=exists_error, exec_errors=exec_errors)
        else:
            raise ValueError("unknown environment: {}".format(run["environment"]))


def _deploy_run_normal(run, inputs, root_path, exists_error=False, exec_errors=False):
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

    Returns
    -------

    """
    files = inputs["files"]
    scripts = inputs["scripts"]
    cmnds = inputs["cmnds"]

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

        logger.info("executing run: {0}: {1}".format(run["id"], run["name"]))

        # create output folder
        outdir = "{0}_{1}".format(run["id"], run["name"])
        if folder.exists(outdir):
            if exists_error:
                logger.critical("aborting run: output dir already exists: {}".format(outdir))
                return False
            logger.info("removing existing output dir: {}".format(outdir))
            folder.rmtree(outdir)
        folder.makedirs(outdir)

        # dump a record of the run configuration to output
        with folder.open(os.path.join(outdir, "config_{}.yaml".format(run["id"])), "w") as f:
            yaml = YAML()
            yaml.dump(run, f)

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
                path = os.path.join(outdir, path)
                for rmpath in list(folder.glob(path)):
                    if folder.exists(rmpath):
                        logger.debug("removing {0} from output".format(rmpath))
                        if folder.isdir(rmpath):
                            folder.rmtree(rmpath)
                        else:
                            folder.remove(rmpath)

        if run["output"]["rename"] is not None:
            for old, new in run["output"]["rename"].items():
                if not old:
                    continue
                renamedir = os.path.join(outdir, "**", "*{}*".format(old))
                for path in folder.glob(renamedir):
                    if folder.isfile(path):
                        newname = os.path.basename(path).replace(old, new)
                        logger.debug("renaming {0} to {1}".format(path, newname))
                        folder.rename(path, newname)

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
readlink -f "." &> /dev/null || readlink_fail=true
if [[ ! "$readlink_fail" = true ]]; then
export PBS_O_WORKDIR=$(readlink -f $PBS_O_WORKDIR)
else
export PBS_O_WORKDIR=$(readlink $PBS_O_WORKDIR)
fi

# Set the number of threads to 1
#   This prevents any system libraries from automatically 
#   using threading.
export OMP_NUM_THREADS=1

echo Running: {run_name}

# load required modules
{load_modules}

if [ -z ${{TMPDIR+x}} ]; then 
    echo "the TEMPDIR variable does not exist"  1>&2
    exit 1
fi
if [ -z "TMPDIR" ]; then
    echo "the TEMPDIR variable is empty"  1>&2
    exit 1
fi
echo "running in: $TMPDIR"
cd $TMPDIR

# copy required input files from $WORKDIR to $TMPDIR
cp -pR {wrkpath}/* $TMPDIR

# main commands to run (in $TMPDIR)
{exec_run}

# remove output files
{remove}

# rename output files
{rename}

# copy output files from $TMPDIR to $WORKDIR
cp -pR $TMPDIR/* {wrkpath}

cd -

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


def _create_qsub(run, wrkpath, cmnds):
    """

    Parameters
    ----------
    qsub: dict
    wrkpath: str
        absolute path of working directory

    Returns
    -------
    qsub: str
        contents of qsub file

    """
    qsub = run["process"]["qsub"]

    # get qsub options
    jobname = qsub["jobname"] if qsub["jobname"] is not None else '{0}_{1}'.format(run["id"], run["name"])
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

    run_name = '{0}_{1}'.format(run["id"], run["name"])

    # get modules to load
    load_modules = "module load " + " ".join(qsub["modules"]) if qsub["modules"] is not None else ""

    # exec runs (in $TMPDIR)
    exec_run = "\n".join(cmnds)

    # remove
    rmlist = []
    rmcmnd = "for path in $(find {}); do if [ -e $path ]; then rm -Rf $path; fi; done"
    if run["output"]["remove"] is not None:
        for rmregex in run["output"]["remove"]:
            rmlist.append(rmcmnd.format(rmregex))
    remove = "\n".join(rmlist)

    # rename
    rnlist = []
    rncmnd = "find . -depth -name '*{inname}*' " \
             "-execdir bash -c 'mv -i \"$1\" \"${{1//{inname}/{outname}}}\"' bash {{}} \;"
    if run["output"]["rename"] is not None:
        for inname, outname in run["output"]["rename"].items():
            rnlist.append(rncmnd.format(inname=inname, outname=outname))
    rename = "\n".join(rnlist)

    out = _qsub_top_template.format(run_name=run_name, wrkpath=wrkpath,
                                    jobname=jobname, walltime=walltime, nnodes=nnodes,
                                    ncores=ncores, nprocs=nprocs, pbs_optional=pbs_optional,
                                    load_modules=load_modules,
                                    exec_run=exec_run, remove=remove, rename=rename)
    return out


# TODO should do source loading more flexibly
# TODO something is not loaded to get emails
_QSUB_CMNDLINE = "source /etc/bashrc; source /etc/profile; qsub run.qsub"


def _deploy_run_qsub(run, inputs, root_path, exists_error=False, exec_errors=False):
    """ deploy run and child runs (recursively)

    Parameters
    ----------
    run: dict
        top level run
    root_path: str or path_like
        the path to resolve (local) relative paths from
    exists_error: bool
        if True, abort tun if the output path already exists
    exec_errors: bool
        if True, abort run if exec commands return with errorcode

    Returns
    -------

    """
    files = inputs["files"]
    scripts = inputs["scripts"]
    cmnds = inputs["cmnds"]

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

        # dump a record of the run configuration to output
        with folder.open(os.path.join(outdir, "config_{}.yaml".format(run["id"])), "w") as f:
            yaml = YAML()
            yaml.dump(run, f)

        for fname, fcontent in files.items():
            with folder.open(os.path.join(outdir, fname), 'w') as f:
                f.write(fcontent)
        for sname, scontent in scripts.items():
            with folder.open(os.path.join(outdir, sname), 'w') as f:
                f.write(scontent)


        # make qsub
        abspath = folder.getabs(outdir)
        qsub = _create_qsub(run, abspath, cmnds)
        with folder.open(os.path.join(outdir, "run.qsub"), 'w') as f:
            f.write(unicode(qsub))

        # run
        cmndline = _QSUB_CMNDLINE
        try:
            folder.exec_cmnd(cmndline, outdir, raise_error=True)
        except RuntimeError:
            if exec_errors:
                logger.critical("aborting run on command line failure: {}".format(cmndline))
                return False
            logger.error("command line failure: {}".format(cmndline))

    return True
