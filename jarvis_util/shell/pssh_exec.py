from .ssh_exec import SshExec
from .local_exec import LocalExec
from jarvis_util.util.hostfile import Hostfile
from .exec_info import ExecInfo, ExecType


class PsshExec:
    def __init__(self, cmd, exec_info):
        self.cmd = cmd
        self.exec_async = exec_info.exec_async
        self.hosts = []
        self.execs_ = []
        if exec_info.hostfile is not None:
            self.hosts = Hostfile(exec_info.hostfile).hosts

        if len(self.hosts):
            for host in self.hosts:
                self.execs_.append(
                    SshExec(cmd, exec_info.mod(hosts=[host], exec_async=True)))
        else:
            self.execs_.append(
                LocalExec(cmd, exec_info))
            return
        if not self.exec_async:
            self.wait()

    def wait(self):
        for exe in self.execs_:
            exe.wait()


class PsshExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.PSSH, **kwargs)

