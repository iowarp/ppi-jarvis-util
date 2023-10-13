"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""

from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec
from .exec_info import ExecInfo, ExecType

class SlurmExec(LocalExec):
    """
    This class contains methods for executing a command in parallel
    using MPI.
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a command through sbatch

        :param cmd: A command (string) to execute
        :param exec_info: Information needed by MPI
        """

        self.cmd = cmd
        self.job_name = exec_info
        self.num_nodes = exec_info.num_noodes
        self.nprocs = exec_info.nprocs
        self.ppn = exec_info.ppn
        self.hostfile = exec_info.hostfile
        self.mpi_env = exec_info.env
        super().__init__(self.slurmcmd(),
                         exec_info.mod(env=exec_info.basic_env))

    def slurmcmd(self):
        params = [f'mpirun -n {self.nprocs}']
        params.append('--oversubscribe')
        if self.ppn is not None:
            params.append(f'-ppn {self.ppn}')
        if len(self.hostfile):
            if self.hostfile.is_subset() or self.hostfile.path is None:
                params.append(f'--host {",".join(self.hostfile.hosts)}')
            else:
                params.append(f'--hostfile {self.hostfile.path}')
        params += [f'-x {key}=\"{val}\"'
                   for key, val in self.mpi_env.items()]
        params.append(self.cmd)
        cmd = ' '.join(params)
        jutil = JutilManager.get_instance()
        if jutil.debug_mpi_exec:
            print(cmd)
        return cmd


class SlurmExecInfo(ExecInfo):
    def __init__(self, job_name=None, num_nodes=1, **kwargs):
        super().__init__(exec_type=ExecType.SLURM, **kwargs)
        allowed_options = ['cpus_per_task', 'time', 'partition', 'mail_type', 'mail_user', 'mem', 'gres', 'exclusive', 'pipeline_file']
        self.job_name = job_name
        self.num_nodes = num_nodes
        self.keys.append(['job_name', 'num_nodes'])
        for key, value in kwargs.items():
            if key in allowed_options:
                setattr(self, key, value)
                self.keys.append(key)
