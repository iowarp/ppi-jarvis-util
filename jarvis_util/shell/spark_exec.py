"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""

from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec
from .exec_info import ExecInfo, ExecType
from abc import abstractmethod


class SparkExec(LocalExec):
    def __init__(self, cmd, master_host, master_port,
                 driver_mem='1g', executor_mem='1g', scratch='/tmp', replication=1,
                 exec_info=None):
        master_url = f'spark://{master_host}:{master_port}'
        sparkcmd = [
            'spark-submit',
            f'--master {master_url}',
            f'--driver-memory {driver_mem}',
            f'--executor-memory {executor_mem}',
            f'--conf spark.speculation=false',
            f'--conf spark.storage.replication={replication}',
            f'--conf spark.local.dir={scratch}',
            cmd
        ]
        sparkcmd = ' '.join(sparkcmd)
        super().__init__(sparkcmd, exec_info)
