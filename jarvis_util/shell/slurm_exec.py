"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""

from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo
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
        self.mem = exec_info.mem
        self.gres = exec_info.gres
        self.exclusive = exec_info.exclusive
        self.host_suffix = exec_info.host_suffix
        self.nodelist = exec_info.nodelist

        super().__init__(self.slurmcmd(),
                         exec_info.mod(env=exec_info.basic_env))

    def generate_sbatch_command(self):
        cmd = "sbatch"

        # Mapping of attribute names to their corresponding sbatch option names
        options_map = {
            'job_name': 'job-name',
            'num_nodes': 'nodes',
            'ppn': 'ntasks-per-node',
            'cpus_per_task': 'cpus-per-task',
            'time': 'time',
            'partition': 'partition',
            'mail_type': 'mail-type',
            'output': 'output',
            'error': 'error',
            'mem': 'mem',
            'gres': 'gres',
            'exclusive': 'exclusive',
            'nodelist': 'nodelist',
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
                           'mail_user', 'mem', 'gres', 'exclusive', 'host_suffix', 'nodelist', 'account']
        self.keys += allowed_options
        # We use ppn, and the output and error file from the base Exec Info
        for key in allowed_options:
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, None)
        self.job_name = job_name
        self.num_nodes = num_nodes

    @staticmethod
    def get_args():
        return [ 
            {
                'name': 'job_name',
                'msg': 'The name given to this job',
                'required': True,
                'pos': False,
                'default': None,
                'class': 'slurm',
                'rank': 1
            },
            {
                'name': 'nnodes',
                'msg': 'The number of nodes to execute the pipeline on',
                'required': True,
                'pos': False,
                'default': None,
                'class': 'slurm',
                'rank': 1
            },
            {
                'name': 'slurm',
                'msg': 'This is the slurm job submitter',
                'type': bool,
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm',
                'rank': 1
            },
            {
                'name': 'slurm_host',
                'msg': 'This is the slurm job receiver (internal, never set manually)',
                'type': bool,
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm',
                'rank': 10
            },
            {
                'name': 'account',
                'msg': 'The account to use for the job',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'ppn',
                'msg': 'The number of processes per node',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'cpus_per_task',
                'msg': 'Advise the Slurm controller that ensuing job will require ncpus number of processors per task',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'time',
                'msg': 'Maximum time aloted to the job',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'partition',
                'msg': 'The partition in which to allocate the nodes',
                'required': False,
                'pos': False,
                'default': 'compute',
                'class': 'slurm'
            },
            {
                'name': 'mail_type',
                'msg': 'When to email users of the status of the job',
                'required': False,
                'pos': False,
                'default': None,
                'choices': ['NONE', 'BEGIN', 'END', 'FAIL', 'REQUEUE', 'ALL'],
                'class': 'slurm'
            },
            {
                'name': 'mail_user',
                'msg': 'What email to use',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'output_file',
                'msg': 'File to write all output messages',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'error_file',
                'msg': 'File to write all error messages',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'memory',
                'msg': 'Amount of memory to request for the job',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'gres',
                'msg': 'A comma-delimited list of generic consumable resources, like gpus',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'exclusive',
                'msg': 'Request the nodes exclusively',
                'required': False,
                'pos': False,
                'default': True,
                'class': 'slurm'
            },
            {
                'name': 'host_suffix',
                'msg': 'Append suffix to all hosts in hostfile',
                'required': False,
                'pos': False,
                'default': None,
                'class': 'slurm'
            },
            {
                'name': 'nodelist',
                'msg': 'A list of nodes to run the job on, exp: ares-comp-[10-14],ares-comp-15',
                'required': False,
                'pos': False,
                'type': str,
                'default': None,
                'class': 'slurm'
            }
        ]

    @staticmethod
    def from_kwargs(kwargs):
        SlurmExecInfo(
            account=kwargs['account'],
            job_name=kwargs['job_name'],
            num_nodes=kwargs['nnodes'],
            ppn=kwargs['ppn'],
            cpus_per_task=kwargs['cpus_per_task'],
            time=kwargs['time'],
            partition=kwargs['partition'],
            mail_type=kwargs['mail_type'],
            mail_user=kwargs['mail_user'],
            pipe_stdout=kwargs['output_file'],
            pipe_stderr=kwargs['error_file'],
            mem=kwargs['memory'],
            gres=kwargs['gres'],
            exclusive=kwargs['exclusive'],
            host_suffix=kwargs['host_suffix'],
            nodelist=kwargs['nodelist']
        )


class SlurmHostfile(LocalExec):
    def __init__(self, file_location, host_suffix=None):
        self.file_location = file_location
        self.host_suffix = host_suffix
        cmd = f'scontrol show hostnames $SLURM_JOB_NODELIST > {file_location}'
        super().__init__(cmd, LocalExecInfo())
        if host_suffix is not None:
            with open(file_location, 'r', encoding='utf-8') as fp:
                lines = fp.read().splitlines()
                lines = [f'{line}{host_suffix}' for line in lines]
            with open(file_location, 'w', encoding='utf-8') as fp:
                fp.write('\n'.join(lines))
                fp.write('\n')
