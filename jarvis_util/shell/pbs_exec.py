"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""

from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec
from .exec_info import ExecInfo, ExecType


class PbsExec(LocalExec):
    """
    This class contains methods for executing a command
    through the PBS scheduler
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a command through qsub
        :param cmd: A command (string) to execute
        :param exec_info: Information needed by qsub
        """
        self.cmd = cmd
        self.interactive = exec_info.interactive
        self.nnodes = exec_info.nnodes
        self.system = exec_info.system
        self.filesystems = exec_info.filesystems
        self.walltime = exec_info.walltime
        self.account = exec_info.account
        self.queue = exec_info.queue
        self.env_vars = exec_info.env_vars
        super().__init__(self.pbscmd(),
                         exec_info.mod(env=exec_info.basic_env))

    def generate_qsub_command(self):
        cmd = 'qsub'

        if self.interactive:
            cmd += ' -I'

        equal_map = {
            'filesystems': 'l filesystems',
            'walltime': 'l walltime',
        }

        non_equal_map ={
            'account': 'A',
            'queue': 'q',
            'env_vars' : '-v'
        }

        if self.nnodes and self.system:
            cmd += f' -l select={self.nnodes}:system={self.system}'
        elif self.nnodes:
            cmd += f' -l select={self.nnodes}'
        else:
            raise ValueError("System defined without select value.")

        for attr, option in equal_map.items():
            value = getattr(self, attr)
            if value is not None:
                cmd += f' -{option}={value}'

        for attr, option in non_equal_map.items():
            value = getattr(self, attr)
            if value is not None:
                cmd += f' -{option} {value}'

        cmd += f' -- {self.cmd}'
        return cmd

    def pbscmd(self):
        cmd = self.generate_qsub_command()
        jutil = JutilManager.get_instance()
        if jutil.debug_pbs:
            print(cmd)
        return cmd


class PbsExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.PBS, **kwargs)
        allowed_options = ['interactive', 'nnodes', 'system', 'filesystems',
                           'walltime', 'account', 'queue', 'env_vars']
        self.keys += allowed_options
        # We use output and error file from the base Exec Info
        for key in allowed_options:
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, None)
