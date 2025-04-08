"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""
from jarvis_util.shell.filesystem import Chmod
from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo
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

        self.bash_script = exec_info.bash_script

        jarvis_comma_list = ','.join(exec_info.basic_env.keys())
        if self.env_vars:
            self.env_vars = f'{self.env_vars},{jarvis_comma_list}'
        else:
            self.env_vars = jarvis_comma_list

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
            'env_vars' : 'v'
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

        cmd += f' -- \"{self.bash_script}\"'
        return cmd

    def pbscmd(self):

        script = ['#!/bin/bash',
                 f'{self.cmd}']

        with open(self.bash_script, mode='w', encoding='utf-8') as f:
            f.write('\n'.join(script))

        Chmod(self.bash_script, "+x")

        cmd = self.generate_qsub_command()
        jutil = JutilManager.get_instance()
        if jutil.debug_pbs:
            print(cmd)
        return cmd


class PbsExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.PBS, **kwargs)
        allowed_options = ['interactive', 'nnodes', 'system', 'filesystems',
                           'walltime', 'account', 'queue', 'env_vars', 'bash_script']
        self.keys += allowed_options
        # We use output and error file from the base Exec Info
        for key in allowed_options:
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, None)

    @staticmethod
    def get_args():
        return [
            {
                'name': 'nnodes',
                'msg': 'The number of nodes to execute the pipeline on',
                'required': True,
                'pos': False,
                'default': 1,
                'class': 'pbs',
                'rank': 1
            },
            {
                'name': 'pbs',
                'msg': 'This is the pbs job submitter',
                'type': bool,
                'required': False,
                'pos': False,
                'default': None,
                'class': 'pbs',
                'rank': 1
            },
            {
                'name': 'pbs_host',
                'msg': 'This is the pbs job receiver (internal, never set manually)',
                'type': bool,
                'required': False,
                'pos': False,
                'default': None,
                'class': 'pbs',
                'rank': 10
            },
            {
                'name': 'system',
                'msg': 'The type of system to allocate the nodes on',
                'required': False,
                'pos': False,
                'default': 'polaris',
                'class': 'pbs'
            },
            {
                'name': 'filesystems',
                'msg': 'The filesystem to be used (e.g. home:grand)',
                'required': False,
                'pos': False,
                'default': 'home:grand',
                'class': 'pbs'
            },
            {
                'name': 'time',
                'msg': 'Maximum time allotted to the job',
                'required': False,
                'pos': False,
                'default': '00:10:00',
                'aliases': ['walltime'],
                'class': 'pbs'
            },
            {
                'name': 'account',
                'msg': 'Account used for job submission',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'pbs'
            },
            {
                'name': 'queue',
                'msg': 'Queue in which to submit the job',
                'required': False,
                'pos': False,
                'default': 'debug-scaling',
                'class': 'pbs'
            },
            {
                'name': 'interactive',
                'msg': 'Submit the job in interactive mode',
                'required': False,
                'pos': False,
                'default': False,
                'type': bool,
                'class': 'pbs'
            },
            {
                'name': 'env_vars',
                'msg': 'Environmental variables to pass through PBS. '
                       'Comma separated list of strings of the form variable or variable=value',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'pbs'
            },
            {
                'name': 'polaris',
                'msg': 'Submit using polaris',
                'required': False,
                'pos': False,
                'default': False,
                'type': bool,
                'class': 'pbs'
            },
        ]

    @staticmethod
    def from_kwargs(kwargs, script_location):
        return PbsExecInfo(
            nnodes=kwargs['nnodes'],
            system=kwargs['system'],
            filesystems=kwargs['filesystems'],
            walltime=kwargs['walltime'],
            account=kwargs['account'],
            queue=kwargs['queue'],
            interactive=kwargs['interactive'],
            bash_script=script_location
        )
