import pytest
from jsonextended import edict, utils
from jsonschema import ValidationError

from atomic_hpc.config_yaml import _format_config_yaml, _find_run_dependancies, runs_from_config

example_file_minimal = """
runs:
  - id: 1
    name: run1
"""

example_file_maximal = """
defaults:
    description: quantum-espresso run
    scripts:
        - _path/to/script1.in
        - _path/to/script2.in
    variables:
        var1: value
        var2: value
        nprocs: 2
    files:
        file1: _path/to/file1
    outpath: _path/to/top/level/output  
    environment: qsub
    local:  
        run:
            - mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out
    qsub:
        cores_per_node: 16  
        nnodes: 1     
        walltime: 1:00:00
        queue: queue_name
        email: true
        modules:
            - quantum-espresso
            - intel-suite
            - mpi
        before_run:
            - ./script1.in
        run: 
            - mpiexec pw.x -i script2.in > main.qe.scf.out  
        from_temp:
            - .other.out
        after_run:
    cleanup:
        remove:
            - tmp/
        aliases:
            .other.out: .other.qe.out

runs:
  - id: 1
    name: run1
    variables:
        var1: overridevalue
  - id: 2
    name: run2
    scripts: 
        - _path/to/other/script1.in
        - _path/to/script2.in
    variables:
        var2: overridevalue
    requires: 1

"""

example_output = [
    {
        "description": "quantum-espresso run",
        "requires": None,
        "scripts": [
            "_path/to/script1.in",
            "_path/to/script2.in"
        ],
        "files": {
            "file1": "_path/to/file1"
        },
        "variables": {
            "var1": "overridevalue",
            "var2": "value",
            "nprocs": 2
        },
        "cleanup": {
            "aliases": {
                ".other.out": ".other.qe.out"
            },
            "remove": [
                "tmp/"
            ]
        },
        "outpath": "_path/to/top/level/output",
        "environment": "qsub",
        "qsub": {
            "cores_per_node": 16,
            "nnodes": 1,
            "walltime": "1:00:00",
            "queue": "queue_name",
            "email": True,
            "modules": [
                "quantum-espresso",
                "intel-suite",
                "mpi"
            ],
            "before_run": [
                "./script1.in"
            ],
            "run": [
                "mpiexec pw.x -i script2.in > main.qe.scf.out"
            ],
            "from_temp": [
                ".other.out"
            ],
            "after_run": None
        },
        "local": {
            "run": [
                "mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out"
            ]
        },
        "id": 1,
        "name": "run1"
    },
    {
        "description": "quantum-espresso run",
        "requires": 1,
        "scripts": [
            "_path/to/other/script1.in",
            "_path/to/script2.in"
        ],
        "files": {
            "file1": "_path/to/file1"
        },
        "variables": {
            "var1": "value",
            "var2": "overridevalue",
            "nprocs": 2
        },
        "cleanup": {
            "aliases": {
                ".other.out": ".other.qe.out"
            },
            "remove": [
                "tmp/"
            ]
        },
        "outpath": "_path/to/top/level/output",
        "environment": "qsub",
        "qsub": {
            "cores_per_node": 16,
            "nnodes": 1,
            "walltime": "1:00:00",
            "queue": "queue_name",
            "email": True,
            "modules": [
                "quantum-espresso",
                "intel-suite",
                "mpi"
            ],
            "before_run": [
                "./script1.in"
            ],
            "run": [
                "mpiexec pw.x -i script2.in > main.qe.scf.out"
            ],
            "from_temp": [
                ".other.out"
            ],
            "after_run": None
        },
        "local": {
            "run": [
                "mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out"
            ]
        },
        "id": 2,
        "name": "run2"
    }
]


def test_format_minimal():
    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file_minimal)
    _format_config_yaml(file_obj)


def test_format_maximal():
    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file_maximal)
    output = _format_config_yaml(file_obj)
    # handy for updating
    # import json
    # print(json.dumps(output, indent=4))
    assert edict.diff(output, example_output) == {}


def test_find_dependencies():
    runs1 = [{'id': 1, 'requires': None}]
    assert _find_run_dependancies(runs1) == [{'id': 1, 'requires': None, "children": []}]

    runs2 = [{'id': 1, 'requires': 1}]
    with pytest.raises(ValidationError, message="Expecting ValidationError"):
        _find_run_dependancies(runs2)

    runs3 = [{'id': 1, 'requires': None}, {'id': 2, 'requires': 1}]
    assert _find_run_dependancies(runs3) == [{'id': 1, 'requires': None,
                                              "children": [{'id': 2, 'requires': 1, "children": []}]}]

    runs4 = [{'id': 1, 'requires': None}, {'id': 2, 'requires': 1}, {'id': 3, 'requires': 1}]
    assert _find_run_dependancies(runs4) == [{'id': 1, 'requires': None,
                                              "children": [{'id': 2, 'requires': 1, "children": []},
                                                           {'id': 3, 'requires': 1, "children": []}]}]

    child1 = {'id': 2, 'requires': 1}
    child2 = {'id': 3, 'requires': 2}
    runs5 = [{'id': 1, 'requires': None}, child1, child2]
    assert _find_run_dependancies(runs5) == [{'id': 1, 'requires': None, "children": [child1]}]
    assert child1 == {'id': 2, 'requires': 1, "children": [child2]}

    runs6 = [{'id': 1, 'requires': None}, {'id': 2, 'requires': 3}]
    with pytest.raises(ValidationError, message="Expecting ValidationError"):
        _find_run_dependancies(runs6)

    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file_minimal)
    real_runs = _format_config_yaml(file_obj)
    real_out1 = _find_run_dependancies(real_runs)
    assert len(real_out1) == 1

    file_obj2 = utils.MockPath('config.yml', is_file=True,
                              content=example_file_maximal)
    real_runs2 = _format_config_yaml(file_obj2)
    real_out2 = _find_run_dependancies(real_runs2)
    assert len(real_out2) == 1


def test_runs_from_config():
    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file_minimal)
    assert len(runs_from_config(file_obj)) == 1
    assert hasattr(runs_from_config(file_obj)[0], 'keys')
    assert runs_from_config(file_obj)[0]["name"] == "run1"