from .local_exec import LocalExec
from .pssh_exec import PsshExec
from .mpi_exec import MpiExec


class ExecInfo:
    def __init__(self, nprocs,
                 ppn=None, hostfile=None, env=None):
        self.nprocs = nprocs
        self.ppn = ppn
        self.hostfile = hostfile
        self.env = env


class Exec:
    def __init__(self, cmd, exec_info, collect_output=None, exec_async=False):
        if exec_info.hostfile is None or exec_info.nprocs == 1:
            self.exec_ = LocalExec(cmd,
                                   collect_output=collect_output,
                                   exec_async=exec_async,
                                   env=exec_info.env)
        else:
            self.exec_ = PsshExec(cmd, exec_info,
                                 collect_output=collect_output,
                                 exec_async=exec_async)

    def wait(self):
        self.exec_.wait()
        return self.exec_.exit_code
