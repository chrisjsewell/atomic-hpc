import pytest
from jsonextended import edict, utils
from jsonschema import ValidationError
# import json

from atomic_hpc.config_yaml import _format_config_yaml, _find_run_dependancies

example_file = """
defaults:
    program: quantum-espresso
    compute: scf
    run: hpc
    local:
        nprocs: 2
        mpi: mpi_executable
        prog_options:
            - opt1: value
    hpc:
        nprocs: 8       
        walltime: 1:00:00
        queue: queue_name
        modules:
            - quantum-espresso
            - intel-suite
            - mpi
        prog_options:
            - opt1: value    
    outpath: path/to/top/level/output  
    variables:
        - var1: value
        - var2: value
    paths:
        - frag1: path/to/frag_file

runs:
  - id: 1
    name: run1
    script: path/to/script
    variables:
        - var1: value
  - id: 2
    name: run2
    compute: band
    script: path/to/script
    variables:
        - var1: value
    requires: 1

"""

example_output = [
    {
        "program": "quantum-espresso",
        "compute": "scf",
        "run": "hpc",
        "hpc": {
            "nprocs": 8,
            "walltime": "1:00:00",
            "queue": "queue_name",
            "modules": [
                "quantum-espresso",
                "intel-suite",
                "mpi"
            ],
            "prog_options": [
                {
                    "opt1": "value"
                }
            ]
        },
        "outpath": "path/to/top/level/output",
        "variables": [
            {
                "var1": "value"
            }
        ],
        "paths": [
            {
                "frag1": "path/to/frag_file"
            }
        ],
        "id": 1,
        "name": "run1",
        "script": "path/to/script",
        "requires": None
    },
    {
        "program": "quantum-espresso",
        "compute": "band",
        "run": "hpc",
        "hpc": {
            "nprocs": 8,
            "walltime": "1:00:00",
            "queue": "queue_name",
            "modules": [
                "quantum-espresso",
                "intel-suite",
                "mpi"
            ],
            "prog_options": [
                {
                    "opt1": "value"
                }
            ]
        },
        "outpath": "path/to/top/level/output",
        "variables": [
            {
                "var1": "value"
            }
        ],
        "paths": [
            {
                "frag1": "path/to/frag_file"
            }
        ],
        "id": 2,
        "name": "run2",
        "script": "path/to/script",
        "requires": 1
    }
]


def test_format():
    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file)
    output = _format_config_yaml(file_obj)
    # print(json.dumps(output, indent=4)) # handy for updating
    assert edict.diff(output, example_output) == {}


def test_find_dependencies():
    runs1 = [{'id': 1, 'requires': None}]
    assert _find_run_dependancies(runs1) == {1: {'id': 1, 'requires': None, "children": []}}

    runs2 = [{'id': 1, 'requires': 1}]
    with pytest.raises(ValidationError, message="Expecting ValidationError"):
        _find_run_dependancies(runs2)

    runs3 = [{'id': 1, 'requires': None}, {'id': 2, 'requires': 1}]
    assert _find_run_dependancies(runs3) == {1: {'id': 1, 'requires': None, "children": [2]}}

    runs4 = [{'id': 1, 'requires': None}, {'id': 2, 'requires': 1}, {'id': 3, 'requires': 1}]
    assert _find_run_dependancies(runs4) == {1: {'id': 1, 'requires': None, "children": [2, 3]}}

    child1 = {'id': 2, 'requires': 1}
    runs5 = [{'id': 1, 'requires': None}, child1, {'id': 3, 'requires': 2}]
    assert _find_run_dependancies(runs5) == {1: {'id': 1, 'requires': None, "children": [2]}}
    assert child1 == {'id': 2, 'requires': 1, "children": [3]}

    runs6 = [{'id': 1, 'requires': None}, {'id': 2, 'requires': 3}]
    with pytest.raises(ValidationError, message="Expecting ValidationError"):
        _find_run_dependancies(runs6)

    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file)
    real_runs = _format_config_yaml(file_obj)
    _find_run_dependancies(real_runs)