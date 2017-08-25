"""
module to deploy runs
"""
import glob
import os
import logging
import re
import shutil
from subprocess import Popen, PIPE, STDOUT, SubprocessError

_REGEX_VAR = r"(?:\@v\{[^}]+\})"
_REGEX_FILE = r"(?:\@f\{[^}]+\})"


class change_dir:
    """Context manager for changing the current working directory"""

    def __init__(self, new_path):
        self.newPath = os.path.expanduser(os.path.abspath(str(new_path)))

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)


def _run_cmndline(cmnd, rid, raise_error=True):
    # subprocess.run(cmnd, shell=True, check=True)
    def log_output(pipe):
        for line in iter(pipe.readline, b''):
            logging.info('{}'.format(line.decode("utf-8").strip()))

    process = Popen(cmnd, stdout=PIPE, stderr=STDOUT, shell=True)
    with process.stdout:
        log_output(process.stdout)
    exitcode = process.wait()  # 0 means success
    if exitcode:
        err_msg = "the following line in run {0} caused errorcode {1}: {2}".format(rid, exitcode, cmnd)
        logging.error(err_msg)
        if raise_error:
            raise SubprocessError(err_msg)


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
        script = script.replace(tag, variables[var])
    return cmndline


def _create_scripts(run, config_path):
    """

    Parameters
    ----------
    run: dict
    config_path: str or path_like

    Returns
    -------

    """
    if run["scripts"] is None:
        return []

    outputs = []

    var_error = "error in run {id}: variable name; {var_name} not available to replace in script; {spath}"
    file_error = "error in run {id}: path name; {path_name} not available to replace in script; {spath}"
    file_exist_error = "error in run {id}: file path; {path} does not exist to replace in script; {spath}"

    for scriptdir in run["scripts"]:

        with change_dir(config_path):

            # create main script
            if not os.path.exists(scriptdir):
                raise ValueError('script path does not exist')
            scriptname = os.path.basename(scriptdir)

            with open(scriptdir, 'r') as f:
                script = f.read()

            # insert variables
            for tag in re.findall(_REGEX_VAR, script):
                var = tag[3:-1]
                if run["variables"] is None:
                    raise KeyError(var_error.format(id=run["id"], var_name=var, spath=scriptdir))
                elif var not in run["variables"]:
                    raise KeyError(var_error.format(id=run["id"], var_name=var, spath=scriptdir))
                script = script.replace(tag, run["variables"][var])

            # insert file contents
            for tag in re.findall(_REGEX_FILE, script):
                var = tag[3:-1]
                if run["files"] is None:
                    raise KeyError(file_error.format(id=run["id"], path_name=var, spath=scriptdir))
                if var not in run["files"]:
                    raise KeyError(file_error.format(id=run["id"], path_name=var, spath=scriptdir))
                if not os.path.exists(run["files"][var]):
                    raise KeyError(file_exist_error.format(id=run["id"], path=run["files"][var], spath=scriptdir))
                with open(run["files"][var]) as f:
                    script = script.replace(tag, f.read())

            outputs.append((scriptname, script))

    return outputs


_qsub_top_template = """
#!/bin/bash
#PBS -N {jobname:.14}
#PBS -l walltime={walltime}
#PBS -l select={nnodes}:ncpus={ncores}
#PBS -j oe
{queue_name}
{email_opts}

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

{run_specific}

"""


def _resolve_walltime(walltime):
    """ ensure wall time is in format H:MM:SS

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


def _create_qsub(qsub, id, name):
    """

    Parameters
    ----------
    qsub: dict
    id: int
    name: str

    Returns
    -------

    """
    jobname = '{0}_{1}'.format(id, name)
    walltime = _resolve_walltime(qsub["walltime"])
    nnodes = qsub["nnodes"]
    ncores = qsub["cores_per_node"]
    nprocs = nnodes*ncores
    queue_name = "#PBS -q " + qsub["queue"] if qsub["queue"] is not None else ""
    # Sends email to the submitter when the job begins/ends/aborts
    email_opts = "#PBS -j bae\n" if qsub.get("email", False) else ""
    load_modules = "module load " + " ".join(qsub["modules"]) if qsub["modules"] is not None else ""

    # TODO run specific options
    run_specific = ""

    out = _qsub_top_template.format(jobname=jobname, walltime=walltime, nnodes=nnodes,
                                    ncores=ncores, nprocs=nprocs, queue_name=queue_name,
                                    email_opts=email_opts, load_modules=load_modules, run_specific=run_specific)

    return out


def deploy_runs(top_level_runs, config_path, exists_error=False, separate_dir=False):
    """

    Parameters
    ----------
    top_level_runs: list
        top level runs
    config_path: str or path_like
        the path of the config file
    exists_error: bool
        if True, raise an IOError if the output path already exists
    separate_dir: bool
        for local:
        if True and parent_dir is not None, the run will take place in the parent_dir
        if False and parent_dir is not None, the contents of the parent_dir will be copied to the output folder

    Returns
    -------
    """
    for run in top_level_runs:
        if run["environment"] == "local":
            deploy_run_local(run, config_path, exists_error=exists_error, separate_dir=separate_dir)
        elif run["environment"] == "qsub":
            deploy_run_qsub(run, config_path)
        else:
            raise ValueError("unknown environment: {}".format(run["environment"]))


def deploy_run_local(run, config_path, exists_error=False, parent_dir=None, separate_dir=False):
    """

    Parameters
    ----------
    run: dict
        top level run
    config_path: str or path_like
        the path of the config file
    exists_error: bool
        if True, raise an IOError if the output path already exists
    parent_dir: str or None
        the parent output directory (if dependant on another run's output)
    separate_dir: bool
        if True and parent_dir is not None, the run will take place in the parent_dir
        if False and parent_dir is not None, the contents of the parent_dir will be copied to the output folder

    Returns
    -------

    """
    logging.info("deploying run locally: {0}: {1}".format(run["id"], run["name"]))


    # collect files
    with change_dir(config_path):
        if run["files"] is not None:
            for path in run["files"].values():
                if not os.path.exists(path):
                    raise IOError("run {0} files path does not exist: {1}".format(run["id"], path))
    scripts = _create_scripts(run, config_path)
    cmnds = [_replace_in_cmnd(cmnd, run["variables"], run["id"]) for cmnd in run["local"]["run"]]

    # populate output directory
    with change_dir(config_path):

        # create output folder
        if parent_dir is None or separate_dir:
            outdir = os.path.join(run["outpath"], "{0}_{1}".format(run["id"], run["name"]))
            outdir = os.path.abspath(outdir)
            if os.path.exists(outdir):
                if exists_error:
                    raise IOError("output dir already exisit: {}".format(outdir))
                logging.info("removing existing output: {}".format(outdir))
                shutil.rmtree(outdir)
            if parent_dir is not None:
                shutil.copytree(parent_dir, outdir)
            else:
                os.makedirs(outdir)
        else:
            outdir = parent_dir

        if run["files"] is not None:
            for path in run["files"].values():
                fname = os.path.basename(path)
                shutil.copy(path, os.path.join(outdir, fname))
        for scriptname, script in scripts:
            with open(os.path.join(outdir, scriptname), 'w') as f:
                f.write(script)

    # run commands
    with change_dir(outdir):
        for cmndline in cmnds:
            _run_cmndline(cmndline, run["id"])

        # cleanup output
        if run["cleanup"]["remove"] is not None:
            for path in run["cleanup"]["remove"]:
                if os.path.exists(path):
                    if os.path.isdir(path):
                        logging.debug("removing {0} from output".format(path))
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                for gpath in glob.glob("*{}".format(path)):
                    logging.debug("removing {0} from output".format(gpath))
                    os.remove(gpath)

        if run["cleanup"]["aliases"] is not None:
            for old, new in run["cleanup"]["aliases"].items():
                for path in glob.glob("*{}".format(old)):
                    logging.debug("renaming {0} to {1}".format(path, path[:-(len(old))]+new))
                    os.rename(path, path[:-(len(old))]+new)

    for child_run in run["children"]:
        deploy_run_local(child_run, config_path, parent_dir=outdir, separate_dir=separate_dir)


def deploy_run_qsub(run, config_path):
    """

    Parameters
    ----------
    run: dict
        top level run
    config_path: str or path_like
        the path of the config file

    Returns
    -------

    """
    logging.info("deploying run: {}".format(run["name"]))

    # collect files
    scripts = _create_scripts(run, config_path)

    qsub = _create_qsub(run["qsub"], run["id"], run["name"])


    # populate output directory
    with change_dir(config_path):
        # create output folder
        outdir = os.path.join(run["outpath"], "{0}_{1}".format(run["id"], run["name"]))
        if not os.path.exists(outdir):
            os.makedirs(outdir)


        # with open(os.path.join(outdir, scriptname), 'w') as f:
        #     f.write(script)
        with open(os.path.join(outdir, "run.qsub"), 'w') as f:
            f.write(qsub)


