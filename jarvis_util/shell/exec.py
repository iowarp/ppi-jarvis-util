"""
This module provides mechanisms to execute binaries either locally or
remotely.
"""
from .local_exec import LocalExec
from .pssh_exec import PsshExec
from .pssh_exec import SshExec
from .mpi_exec import MpiVersion, MpichExec, OpenMpiExec, CrayMpichExec
from .exec_info import ExecInfo, ExecType, Executable


class Exec(Executable):
    """
    This class is a factory which wraps around various shell command
    execution stragies, such as MPI and SSH.
    """

    def __init__(self, cmd, exec_info=None):
        """
        Execute a command or list of commands

        :param cmd: list of commands or a single command string
        :param exec_info: Info needed to execute processes locally
        """
        super().__init__()
        if exec_info is None:
            exec_info = ExecInfo()
        exec_type = exec_info.exec_type
        if exec_type == ExecType.LOCAL:
            self.exec_ = LocalExec(cmd, exec_info)
        elif exec_type == ExecType.SSH:
            self.exec_ = SshExec(cmd, exec_info)
        elif exec_type == ExecType.PSSH:
            self.exec_ = PsshExec(cmd, exec_info)
        elif exec_type == ExecType.MPI:
            exec_type = MpiVersion(exec_info).version

        if exec_type == ExecType.MPICH:
            self.exec_ = MpichExec(cmd, exec_info)
        elif exec_type == ExecType.INTEL_MPI:
            self.exec_ = MpichExec(cmd, exec_info)
        elif exec_type == ExecType.OPENMPI:
            self.exec_ = OpenMpiExec(cmd, exec_info)
        elif exec_type == ExecType.CRAY_MPICH:
            self.exec_ = CrayMpichExec(cmd, exec_info)

        self.set_exit_code()
        self.set_output()

    def wait(self):
        self.exec_.wait()
        self.set_output()
        self.set_exit_code()
        return self.exit_code

    def set_output(self):
        self.stdout = self.exec_.stdout
        self.stderr = self.exec_.stderr
        if isinstance(self.stdout, str):
            if hasattr(self.exec_, 'addr'):
                host = self.exec_.addr
            else:
                host = 'localhost'
            self.stdout = {host: self.stdout}
            self.stderr = {host: self.stderr}

    def set_exit_code(self):
        self.exec_.set_exit_code()
        self.exit_code = self.exec_.exit_code
