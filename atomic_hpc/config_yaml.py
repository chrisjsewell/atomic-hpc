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
try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib


_config_schema = {
    "description": "config schema",
    "type": "object",
    "required": ['runs'],
    "properties": {
        "defaults": {"type": "object"},
        "runs": {"type": "array", "uniqueItems": True, "minItems": 1,
                 "items": {"type": "object"}},
    }
}

_process_qsub_schema = {
    "type": "object",
    "required": ["nnodes", "cores_per_node", "walltime", "queue", "modules",
                 "run", "jobname"],
    "properties": {

        "cores_per_node": {"type": "integer"},
        "nnodes": {"type": "integer"},
        "walltime": {"type": "string", "format": "date-time"},
        "queue": {"type": ["string", "null"]},
        "jobname": {"type": ["string", "null"]},
        "email": {"type": ["string", "null"]},
        "modules": {"type": ["array", "null"], "items": {"type": "string"}},

        "run": {"type": ["array", "null"], "items": {"type": "string"}},
    },
    "additionalProperties": False,

}

_process_local_schema = {
    "type": "object",
    "required": ["run"],
    "properties": {
        "run": {"type": ["array", "null"], "items": {"type": "string"}},
    },
    "additionalProperties": False,

}

_remote_schema = {
    "type": ["object", "null"],
    "required": ["hostname", "port", "username", "password", "pkey", "key_filename", "timeout"],
    "additionalProperties": False,
    "properties": {
        "hostname": {"type": ["string", "null"]},
        "port": {"type": "integer"},
        "username": {"type": ["string", "null"]},
        "password": {"type": ["string", "null"]},
        "pkey": {"type": ["string", "null"]},
        "key_filename": {"type": ["string", "null"]},
        "timeout": {"type": ["integer", "null"]},
    }
}

_run_schema = {
    "type": "object",
    "required": ["id", "name", "description", "environment", "input", "output", "process"],
    "properties": {

        "id": {"type": "integer"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "environment": {"type": "string", "oneOf": [{"pattern": "qsub"}, {"pattern": "unix"}, {"pattern": "windows"}]},

        "input": {"type": ["object", "null"],
                  "required": ["remote", "path", "scripts", "files", "variables"],
                  "additionalProperties": False,
                  "properties": {
                        "remote": _remote_schema,
                        "path": {"type": ["string", "null"]},
                        "scripts": {"type": ["array", "null"], "items": {"type": "string"}},
                        "files": {"type": ["object", "null"], "patternProperties": {'.+': {"type": "string"}}},
                        "variables": {"type": ["object", "null"]},
                  }},

        "output": {"type": "object",
                   "required": ["remote", "path", "remove", "rename"],
                   "additionalProperties": False,
                   "properties": {
                       "remote": _remote_schema,
                       "path": {"type": ["string", "null"]},
                       "remove":  {"type": ["array", "null"], "items": {"type": "string"}},
                       "rename": {"type": ["object", "null"], "patternProperties": {'.+': {"type": "string"}}}
                        },
                   },

        "process": {"type": "object",
                    "required": ["qsub", "unix", "windows"],
                    "additionalProperties": False,
                    "properties": {
                        "qsub": _process_qsub_schema,
                        "unix": _process_local_schema,
                        "windows": _process_local_schema}
                    },
    },
    "additionalProperties": False,

}

_global_defaults = {
    "description": "",
    "environment": "unix",

    "input": {
        "path": None,
        "scripts": None,
        "files": None,
        "variables": None,
        "remote": {
            "hostname": None,
            "port": 22,
            "username": None,
            "password": None,
            "pkey": None,
            "key_filename": None,
            "timeout": None,
        },
    },

    "output": {
        "remote": None,
        "path": "output",
        "remove": None,
        "rename": None,
        "remote": {
            "hostname": None,
            "port": 22,
            "username": None,
            "password": None,
            "pkey": None,
            "key_filename": None,
            "timeout": None,
        },
    },

    "process": {
        "unix": {"run": None},
        "windows": {"run": None},
        "qsub": {
            "jobname": None,
            "cores_per_node": 16,
            "nnodes": 1,
            "walltime": "24:00:00",
            "queue": None,
            "email": None,
            "modules": None,
            "run": None,
        },
    }
}


def format_config_yaml(file_obj):
    """read config, merge defaults into runs, for each run: drop local or qsub and check against schema

    Parameters
    ----------
    file_obj : str or file_like

    """
    logging.info("reading config: {}".format(file_obj))

    if isinstance(file_obj, basestring):
        file_obj = pathlib.Path(file_obj)

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

        if new_run["input"] is not None:
            all_none = True
            if new_run["input"]["remote"]["hostname"] is None:
                new_run["input"]["remote"] = None
            for field in ["remote", "path", "scripts", "files", "variables"]:
                if new_run["input"][field] is not None:
                    all_none = False
            if all_none:
                new_run["input"] = None

        if new_run["output"]["remote"]["hostname"] is None:
            new_run["output"]["remote"] = None

        runs.append(new_run)

    ids = edict.filter_keys(runs, ['id'], list_of_dicts=True)
    ids = edict.combine_lists(ids)['id']
    if not len(set(ids)) == len(ids):
        raise ValidationError("the run ids are not unique: {}".format(ids))

    return runs
