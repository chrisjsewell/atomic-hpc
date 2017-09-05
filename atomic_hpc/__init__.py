import logging
from atomic_hpc import utils
try:
    utils.add_loglevel("EXEC", logging.INFO + 1)
except AttributeError:
    pass
logger = logging.getLogger(__name__)

from atomic_hpc import config_yaml, deploy_runs, mockssh, context_folder

__version__ = "0.1.4"

