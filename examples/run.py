import logging
import sys

logging.basicConfig(format='%(name)10s:%(levelname)8s: %(message)s', level=logging.INFO, stream=sys.stdout)

from atomic_hpc.config_yaml import runs_from_config
toplevel = runs_from_config("/Users/cjs14/GitHub/atomic-hpc/examples/basic_remote.yaml")
from atomic_hpc.deploy_runs import deploy_runs
deploy_runs(toplevel, "")
