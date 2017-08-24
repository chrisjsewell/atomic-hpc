from ruamel.yaml import YAML
from jsonschema import validate, ValidationError
from jsonextended import edict

_config_schema = {
    "type": "object",
    "required": ['runs'],
    "properties": {
        "defaults": {"type": "object"},
        "runs": {"type": "array", "uniqueItems": True, "minItems": 1,
                 "items": {"type": "object"}},
    }
}

_run_schema = {
    "type": "object",
    "required": ["program", "compute", "run", "name", "script", "outpath"],
    "properties": {
        "id": {"type": "integer"},
        "program": {"type": "string"},
        "compute": {"type": "string"},
        "name": {"type": "string"},
        "script": {"type": "string"},
        "outpath": {"type": "string"},
        "run": {"type": "string", "oneOf": [{"pattern": "hpc"}, {"pattern": "local"}]},
        "variables": {"type": "array", "items": {"type": "object", "maxProperties": 1}},
        "paths": {"type": "array", "items": {"type": "object", "maxProperties": 1}},
        "requires": {"type": ["integer", "null"]}
        # "requires": {"type": "array", "items": {"type": "integer"}}
    }
}

_hpc_schema = {
    "type": "object",
    "required": ["nprocs", "modules"],
    "properties": {
        "nprocs": {"type": "integer"},
        "modules": {"type": "array", "items": {"type": "string"}},
        "prog_options": {"type": "array", "items": {"type": "object", "maxProperties":1}},
        "queue": {"type": "string"},
        "walltime": {"type": "string", "format": "date-time"}
    }
}
_local_schema = {
    "type": "object",
    "required": ["nprocs"],
    "properties": {
        "nprocs": {"type": "integer"},
        "prog_options": {"type": "array", "items": {"type": "object", "maxProperties": 1}},
        "mpi": {"type": "string"},

    }
}


def _format_config_yaml(file_obj):
    """read config, merge defaults into runs, for each run: drop local or hpc and check against schema

    Parameters
    ----------
    file_obj : file_like

    """

    yaml = YAML()
    dct = yaml.load(file_obj)
    validate(dct, _config_schema)
    runs = []
    defaults = dct.get('defaults', {})
    for i, run in enumerate(dct['runs']):

        new_run = edict.merge([defaults, run], overwrite=True)
        try:
            validate(new_run, _run_schema)
        except ValidationError as err:
            raise ValidationError("error in run #{0} config:\n{1}".format(i+1, err))

        rtype = new_run["run"]
        if rtype == "local":
            new_run.pop("hpc")
            try:
                validate(new_run['local'], _local_schema)
            except ValidationError as err:
                raise ValidationError("error in run #{0} local config:\n{1}".format(i + 1, err))
        else:
            new_run.pop("local")
            try:
                validate(new_run['hpc'], _hpc_schema)
            except ValidationError as err:
                raise ValidationError("error in run #{0} hpc config:\n{1}".format(i+1, err))

        runs.append(new_run)

    ids = edict.filter_keys(runs, ['id'], list_of_dicts=True)
    ids = edict.combine_lists(ids)['id']
    if not len(set(ids)) == len(ids):
        raise ValidationError("the run ids are not unique: {}".format(ids))

    for run in runs:
        if 'requires' in run:
            if not run["requires"] in ids:  # set(run["requires"]).issubset(ids):
                raise ValidationError("error in run id{0}: the requires field id is not present")
        else:
            run['requires'] = None

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
                current_level[run['requires']]['children'].append(run['id'])
                next_level[run['id']] = run
                remaining.remove(run)
        current_level = next_level

    if remaining:
        raise ValidationError('the runs have unresolved dependencies (see requires field): {}'.format(
            [r['id'] for r in remaining]))

    return top_level


def runs_from_config(file_obj):
    """read config, validate and format

    Parameters
    ----------
    file_obj : file_like

    Returns
    -------

    """
    runs = _format_config_yaml(file_obj)
    top_level = _find_run_dependancies(runs)

    return runs, top_level
