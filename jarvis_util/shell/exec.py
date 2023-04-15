from .local_exec import LocalExec
from .pssh_exec import PsshExec
from .pssh_exec import SshExec
from .mpi_exec import MpiExec
from .exec_info import ExecInfo, ExecType


class Exec:
    def __init__(self, cmd, exec_info=None):
        if exec_info is None:
            exec_info = ExecInfo()
        if exec_info.exec_type == ExecType.LOCAL:
            self.exec_ = LocalExec(cmd, exec_info)
        elif exec_info.exec_type == ExecType.SSH:
            self.exec_ = SshExec(cmd, exec_info)
        elif exec_info.exec_type == ExecType.PSSH:
            self.exec_ = PsshExec(cmd, exec_info)
        elif exec_info.exec_type == ExecType.MPI:
            self.exec_ = MpiExec(cmd, exec_info)

    def wait(self):
        self.exec_.wait()
        return self.exec_.exit_code
