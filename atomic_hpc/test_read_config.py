import pytest
from jsonextended import edict, utils
from jsonschema import ValidationError

from atomic_hpc.config_yaml import format_config_yaml

example_file_minimal = """
runs:
  - id: 1
    name: run1
"""

expected_output_minimal = [
    {
        "description": "",
        "environment": "unix",
        "input": None,
        "output": {
            "remote": None,
            "path": "output",
            "remove": None,
            "rename": None
        },
        "process": {
            "unix": {
                "run": None
            },
            "windows": {
                "run": None
            },
            "qsub": {
                "jobname": None,
                "cores_per_node": 16,
                "nnodes": 1,
                "walltime": "24:00:00",
                "queue": None,
                "email": None,
                "modules": None,
                "run": None,
            }
        },
        "id": 1,
        "name": "run1"
    }
]

example_file_maximal = """
defaults:
    description: quantum-espresso run
    environment: qsub

    input:
        remote:
            hostname: login.cx1.hpc.imperial.ac.uk
            username: cjs14
        variables:
            var1: value
            var2: value
            nprocs: 2
        files:
            file1: path/to/file1
        scripts:
        - path/to/script1.in
        - path/to/script2.in

    output:
        remote:
            hostname: login.cx1.hpc.imperial.ac.uk
            username: cjs14
        path: path/to/top/level/output 
        remove:
            - tmp/
        rename:
            .other.out: .other.qe.out
    
    process:
        unix:
            run:
                - mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out
        qsub:
            cores_per_node: 16  
            nnodes: 1     
            walltime: 1:00:00
            queue: queue_name
            email: bob@hotmail.com
            modules:
                - quantum-espresso
                - intel-suite
                - mpi
            run: 
                - mpiexec pw.x -i script2.in > main.qe.scf.out  

runs:
  - id: 1
    name: run1
    input:
        variables:
            var1: overridevalue
  - id: 2
    name: run2
    input:
        scripts: 
            - path/to/other/script1.in
            - path/to/script2.in
        variables:
            var2: overridevalue

"""

expected_output_maximal = [
    {
        "description": "quantum-espresso run",
        "environment": "qsub",
        "input": {
            "path": None,
            "scripts": [
                "path/to/script1.in",
                "path/to/script2.in"
            ],
            "files": {
                "file1": "path/to/file1"
            },
            "variables": {
                "var1": "overridevalue",
                "var2": "value",
                "nprocs": 2
            },
            "remote": {
                "hostname": "login.cx1.hpc.imperial.ac.uk",
                "port": 22,
                "username": "cjs14",
                "password": None,
                "pkey": None,
                "key_filename": None,
                "timeout": None
            }
        },
        "output": {
            "remote": {
                "hostname": "login.cx1.hpc.imperial.ac.uk",
                "port": 22,
                "username": "cjs14",
                "password": None,
                "pkey": None,
                "key_filename": None,
                "timeout": None
            },
            "path": "path/to/top/level/output",
            "remove": [
                "tmp/"
            ],
            "rename": {
                ".other.out": ".other.qe.out"
            }
        },
        "process": {
            "unix": {
                "run": [
                    "mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out"
                ]
            },
            "windows": {
                "run": None
            },
            "qsub": {
                "jobname": None,
                "cores_per_node": 16,
                "nnodes": 1,
                "walltime": "1:00:00",
                "queue": "queue_name",
                "email": "bob@hotmail.com",
                "modules": [
                    "quantum-espresso",
                    "intel-suite",
                    "mpi"
                ],
                "run": [
                    "mpiexec pw.x -i script2.in > main.qe.scf.out"
                ],
            }
        },
        "id": 1,
        "name": "run1"
    },
    {
        "description": "quantum-espresso run",
        "environment": "qsub",
        "input": {
            "path": None,
            "scripts": [
                "path/to/other/script1.in",
                "path/to/script2.in"
            ],
            "files": {
                "file1": "path/to/file1"
            },
            "variables": {
                "var1": "value",
                "var2": "overridevalue",
                "nprocs": 2
            },
            "remote": {
                "hostname": "login.cx1.hpc.imperial.ac.uk",
                "port": 22,
                "username": "cjs14",
                "password": None,
                "pkey": None,
                "key_filename": None,
                "timeout": None
            }
        },
        "output": {
            "remote": {
                "hostname": "login.cx1.hpc.imperial.ac.uk",
                "port": 22,
                "username": "cjs14",
                "password": None,
                "pkey": None,
                "key_filename": None,
                "timeout": None
            },
            "path": "path/to/top/level/output",
            "remove": [
                "tmp/"
            ],
            "rename": {
                ".other.out": ".other.qe.out"
            }
        },
        "process": {
            "unix": {
                "run": [
                    "mpirun -np @v{nprocs} pw.x -i script1.in > main.qe.scf.out"
                ]
            },
            "windows": {
                "run": None
            },
            "qsub": {
                "jobname": None,
                "cores_per_node": 16,
                "nnodes": 1,
                "walltime": "1:00:00",
                "queue": "queue_name",
                "email": "bob@hotmail.com",
                "modules": [
                    "quantum-espresso",
                    "intel-suite",
                    "mpi"
                ],
                "run": [
                    "mpiexec pw.x -i script2.in > main.qe.scf.out"
                ],
            }
        },
        "id": 2,
        "name": "run2"
    }
]


def test_format_minimal():
    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file_minimal)
    output = format_config_yaml(file_obj)
    # handy for updating
    # import json
    # print(json.dumps(output, indent=4))
    assert edict.diff(output, expected_output_minimal) == {}


def test_format_maximal():
    file_obj = utils.MockPath('config.yml', is_file=True,
                              content=example_file_maximal)
    output = format_config_yaml(file_obj)
    # handy for updating
    # import json
    # print(json.dumps(output, indent=4))
    assert edict.diff(output, expected_output_maximal) == {}

