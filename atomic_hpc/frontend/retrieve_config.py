#!/usr/bin/env python
import os
import sys
import argparse
import logging
import logging.handlers
from atomic_hpc import __version__
from atomic_hpc.config_yaml import format_config_yaml
from atomic_hpc.deploy_runs import retrieve_outputs
from atomic_hpc.utils import cmndline_prompt, str2intlist
from jsonschema import ValidationError

logger = logging.getLogger('atomic_hpc.retrieve_config')


def run(fpath, runs=None, outpath="", basepath="", log_level='INFO',
        if_exists="abort", path_regex="*", ignore_regex=None):
    """

    Parameters
    ----------
    fpath: str
    runs: list of ints or None
    outpath: str
    basepath: str
    log_level: str
    if_exists: ["abort", "remove", "use"]
        either; raise an IOError if the output path already exists,
        remove the output path or use it without change
   path_regex: str
        regex to search for files
   ignore_regex: None or list of str
        file regexes to ignore (not copy)

    Returns
    -------

    """
    if log_level.upper() == "DEBUG_FULL":
        log_level = "DEBUG"
        filter_ext = False
    else:
        filter_ext = True

    root = logging.getLogger()
    root.handlers = []  # remove any existing handlers
    root.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(getattr(logging, log_level.upper()))
    # TODO align messages
    formatter = logging.Formatter('%(levelname)8s: %(module)10s: %(message)s')
    stream_handler.setFormatter(formatter)
    stream_handler.propogate = False
    if filter_ext:
        stream_handler.addFilter(logging.Filter('atomic_hpc'))
    root.addHandler(stream_handler)

    # TODO add option for file logger
    # file_handler = logging.FileHandler(
    # os.path.join(outdir, ipynb_name + '.config.log'), 'w')
    # file_handler.setLevel(getattr(logging, log_level.upper()))
    # file_handler.setFormatter(formatter)
    # file_handler.propogate = False
    # file_handler.addFilter(logging.Filter('atomic_hpc'))
    # root.addHandler(file_handler)

    fpath = os.path.abspath(fpath)
    basepath = os.path.abspath(basepath)

    try:
        runs_to_deploy = format_config_yaml(fpath, errormsg_only=True)
    except ValidationError as err:
        logger.critical(err)
        return
    if runs is not None:
        runs_to_deploy = [r for r in runs_to_deploy if r["id"] in runs]

    try:
        retrieve_outputs(
            runs_to_deploy, outpath, basepath, if_exists=if_exists,
            path_regex=path_regex, ignore_regex=ignore_regex)
    except RuntimeError as err:
        logger.critical(err)


class ErrorParser(argparse.ArgumentParser):
    """
    on error; print help string
    """

    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def main(sys_args=None):

    if sys_args is None:
        sys_args = sys.argv[1:]

    parser = ErrorParser(
        description=(
            'retrieve outputs created by processes '
            'run from a config.yaml file')
    )
    parser.add_argument("configpath", type=str,
                        help='yaml config file path', metavar='filepath')
    parser.add_argument("-o", "--outpath", type=str, metavar='str',
                        help='base path to output to',
                        default=os.path.join(os.getcwd(), "outputs"))
    parser.add_argument("-b", "--basepath", type=str, metavar='str',
                        help=('path to use when resolving relative paths '
                              'in the config file'),
                        default=os.getcwd())
    parser.add_argument('-r', '--runs', type=str2intlist, default=None,
                        help=("subset of run ids, in delimited list, "
                              "e.g. -r 1,5-6,7"))
    parser.add_argument("-ie", "--if-exists", type=str, default='abort',
                        choices=['abort', 'remove', 'use'],
                        help=(
                            "if a run's output directory already exists "
                            "either; abort the run, remove its contents, "
                            "or use it without removal "
                            "(existing files will be overwritten)"))
    parser.add_argument("-log", "--log-level", type=str, default='info',
                        choices=['debug_full', 'debug', 'info',
                                 'exec', 'warning', 'error'],
                        help=(
                            'the logging level to output to screen/file '
                            '(NB: debug_full '
                            'allows logging from external packages)'))
    parser.add_argument("-rx", "--path-regex", type=str, metavar='str',
                        help='file/folder regex to retrieve', default="*")
    parser.add_argument("-ix", "--ignore-regex", type=str,
                        metavar='str', nargs='*',
                        help='file regexes to ignore')
    # parser.add_argument("--test-run", action="store_true",
    #                     help=('do not run any executables '
    # '(only create directories and create/copy files)'))
    parser.add_argument('--version', action='version', version=__version__)

    args = parser.parse_args(sys_args)
    options = vars(args)

    if options["if_exists"] == "remove":
        if not cmndline_prompt(
                "Are you sure you wish to remove existing outputs?"):
            sys.exit()
    elif options["if_exists"] == "use":
        if not cmndline_prompt(
                "Are you sure you wish to overwrite existing outputs?"):
            sys.exit()

    filepath = options.pop('configpath')
    run(filepath, **options)
