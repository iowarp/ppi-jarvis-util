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
    This class contains methods for executing a command
    through the Slurm scheduler
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a command through sbatch
        :param cmd: A command (string) to execute
        :param exec_info: Information needed by sbatch
        """
        self.cmd = cmd
        self.job_name = exec_info.job_name
        self.num_nodes = exec_info.num_nodes
        self.ppn = exec_info.ppn
        self.cpus_per_task = exec_info.cpus_per_task
        self.time = exec_info.time
        self.partition = exec_info.partition
        self.mail_type = exec_info.mail_type
        self.output = exec_info.pipe_stdout
        self.error = exec_info.pipe_stderr
        self.memory = exec_info.memory
        self.gres = exec_info.gres
        self.exclusive = exec_info.exclusive

        super().__init__(self.slurmcmd(),
                         exec_info.mod(env=exec_info.basic_env))

    def generate_sbatch_command(self):
        cmd = "sbatch"

        # Mapping of attribute names to their corresponding sbatch option names
        options_map = {
            'job_name': 'job-name',
            'num_nodes': 'nodes',
            'ppn': 'ntasks',
            'cpus_per_task': 'cpus-per-task',
            'time': 'time',
            'partition': 'partition',
            'mail_type': 'mail-type',
            'output': 'output',
            'error': 'error',
            'memory': 'mem',
            'gres': 'gres',
            'exclusive': 'exclusive'
        }

        for attr, option in options_map.items():
            value = getattr(self, attr)
            if value is not None:
                if value is True:  # For options like 'exclusive' that don't take a value
                    cmd += f" --{option}"
                else:
                    cmd += f" --{option}={value}"

        cmd += f" {self.cmd}"
        return cmd

    def slurmcmd(self):
        cmd = self.generate_sbatch_command()
        jutil = JutilManager.get_instance()
        if jutil.debug_slurm:
            print(cmd)
        return cmd


class SlurmExecInfo(ExecInfo):
    def __init__(self, job_name=None, num_nodes=1, **kwargs):
        super().__init__(exec_type=ExecType.SLURM, **kwargs)
        allowed_options = ['job_name', 'num_nodes', 'cpus_per_task', 'time', 'partition', 'mail_type',
                           'mail_user', 'mem', 'gres', 'exclusive']
        self.keys += allowed_options
        # We use ppn, and the output and error file from the base Exec Info
        self.job_name = job_name
        self.num_nodes = num_nodes
        for key in allowed_options:
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, None)
