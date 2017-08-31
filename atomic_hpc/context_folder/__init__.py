""" a context manager to handle file and process execution in an environment agnostic manner
"""

from atomic_hpc.context_folder.base import change_dir
from atomic_hpc.context_folder.local import LocalPath
from atomic_hpc.context_folder.remote import RemotePath
