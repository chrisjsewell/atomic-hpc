import logging
import os
from ruamel.yaml import YAML
from jsonschema import validate, ValidationError
from jsonextended import edict

# python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str

# TODO split local into darwin, windows, linux
# TODO have (global) pre run commands

_config_schema = {
    "type": "object",
    "required": ['runs'],
    "properties": {
        "defaults": {"type": "object"},
        "runs": {"type": "array", "uniqueItems": True, "minItems": 1,
                 "items": {"type": "object"}},
    }
}

_qsub_schema = {
    "type": "object",
    "required": ["nnodes", "cores_per_node", "walltime", "queue", "modules",
                 "before_run", "run", "from_temp", "after_run"],
    "properties": {

        "cores_per_node": {"type": "integer"},
        "nnodes": {"type": "integer"},
        "walltime": {"type": "string", "format": "date-time"},
        "queue": {"type": ["string", "null"]},
        "email": {"type": "boolean"},
        "modules": {"type": ["array", "null"], "items": {"type": "string"}},

        "before_run": {"type": ["array", "null"], "items": {"type": "string"}},
        "run": {"type": ["array", "null"], "items": {"type": "string"}},
        "from_temp": {"type": ["array", "null"], "items": {"type": "string"}},
        "after_run": {"type": ["array", "null"], "items": {"type": "string"}},
    },
    "additionalProperties": False,

}
_local_schema = {
    "type": "object",
    "required": ["run"],
    "properties": {

        #        "nprocs": {"type": "integer"},
        "run": {"type": ["array", "null"], "items": {"type": "string"}},

    },
    "additionalProperties": False,

}

_run_schema = {
    "type": "object",
    "required": ["id", "name", "description", "requires", "scripts", "files", "variables", "cleanup",
                 "outpath", "environment"],
    "properties": {

        "id": {"type": "integer"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "requires": {"type": ["integer", "null"]},

        "scripts": {"type": ["array", "null"], "items": {"type": "string"}},
        "files": {"type": ["object", "null"], "patternProperties": {'.+': {"type": "string"}}},
        "variables": {"type": ["object", "null"]},

        "outpath": {"type": "string"},
        "cleanup": {"type": "object", "required": ["remove", "aliases"],
                    "properties": {
                        "remove":  {"type": ["array", "null"], "items": {"type": "string"}},
                        "aliases": {"type": ["object", "null"], "patternProperties": {'.+': {"type": "string"}}}
                        },
                    "additionalProperties": False,
                    },
        "environment": {"type": "string", "oneOf": [{"pattern": "qsub"}, {"pattern": "local"}]},

        "qsub": _qsub_schema,
        "local": _local_schema,
    },
    "additionalProperties": False,

}


_global_defaults = {
    "description": "",
    "requires": None,

    "scripts": None,
    "files": None,
    "variables": None,

    "outpath": "output",
    "cleanup": {"remove": None, "aliases": None},

    "environment": "local",

    "local": {
        #        "nprocs": 1,
        "run": None
    },

    "qsub": {
        "cores_per_node": 16,
        "nnodes": 1,
        "walltime": "24:00:00",
        "queue": None,
        "email": True,
        "modules": None,

        "before_run": None,
        "run": None,
        "from_temp": None,
        "after_run": None,
    }

}


def _format_config_yaml(file_obj):
    """read config, merge defaults into runs, for each run: drop local or qsub and check against schema

    Parameters
    ----------
    file_obj : file_like

    """
    logging.info("reading yaml")

    ryaml = YAML()
    dct = ryaml.load(file_obj)
    validate(dct, _config_schema)
    runs = []
    defaults = edict.merge([_global_defaults, dct.get('defaults', {})], overwrite=True)

    for i, run in enumerate(dct['runs']):

        new_run = edict.merge([defaults, run], overwrite=True)
        try:
            validate(new_run, _run_schema)
        except ValidationError as err:
            raise ValidationError("error in run #{0} config:\n{1}".format(i + 1, err))

        # rtype = new_run["environment"]
        # if rtype == "local":
        #     new_run.pop("qsub")
        #     try:
        #         validate(new_run['local'], _local_schema)
        #     except ValidationError as err:
        #         raise ValidationError("error in run #{0} local environment config:\n{1}".format(i + 1, err))
        # else:
        #     new_run.pop("local")
        #     try:
        #         validate(new_run['qsub'], _qsub_schema)
        #     except ValidationError as err:
        #         raise ValidationError("error in run #{0} qsub environment config:\n{1}".format(i + 1, err))

        runs.append(new_run)

    ids = edict.filter_keys(runs, ['id'], list_of_dicts=True)
    ids = edict.combine_lists(ids)['id']
    if not len(set(ids)) == len(ids):
        raise ValidationError("the run ids are not unique: {}".format(ids))

    for run in runs:
        if run["requires"] is not None:
            if not run["requires"] in ids:  # set(run["requires"]).issubset(ids):
                raise ValidationError("error in run id {0}: the requires field id is not present".format(run["id"]))

    return runs


def _find_run_dependancies(runs):
    """ populate run children fields and return a top-level containing runs with no dependencies

    Parameters
    ----------
    runs: list of dicts
        from format_config_yaml (items must have id, requires keys)

    Returns
    -------

    """
    logging.info("resolving run dependencies")

    top_level = {}

    if not runs:
        return top_level

    remaining = []

    for run in runs:
        run['children'] = run.get('children', [])
        if run["requires"] is None:
            top_level[run['id']] = run
        else:
            remaining.append(run)

    if not top_level:
        raise ValidationError('runs have unresolved dependencies (see requires field)')

    current_level = top_level
    while current_level:
        next_level = {}
        for run in remaining[:]:
            if run['requires'] in current_level:
                current_level[run['requires']]['children'].append(run)
                next_level[run['id']] = run
                remaining.remove(run)
        current_level = next_level

    if remaining:
        raise ValidationError('the runs have unresolved dependencies, check requires field in runs: {}'.format(
            [r['id'] for r in remaining]))

    return list(top_level.values())


# def _get_config_dir(file_obj):
#     """get directory of config"""
#     if hasattr(file_obj, "parent"):
#         return file_obj.parent
#     elif isinstance(file_obj, basestring):
#         return os.path.dirname(file_obj)
#     else:
#         raise IOError("cannot get parent directory of file object")


def runs_from_config(file_obj):
    """read config, validate and format

    Parameters
    ----------
    file_obj : file_like

    Returns
    -------

    """
    # filedir = _get_config_dir(file_obj)
    runs = _format_config_yaml(file_obj)
    top_level = _find_run_dependancies(runs)

    return top_level
