from .ssh_exec import SshExec
from .local_exec import LocalExec
from jarvis_util.util.hostfile import Hostfile
from .exec_info import ExecInfo, ExecType, Executable


class PsshExec(Executable):
    def __init__(self, cmd, exec_info):
        super().__init__()
        self.cmd = cmd
        self.exec_async = exec_info.exec_async
        self.hosts = []
        self.execs_ = []
        if exec_info.hostfile is not None:
            self.hosts = exec_info.hostfile.hosts

        if len(self.hosts):
            for host in self.hosts:
                print(f"PSSH HOST: {host}")
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
        self.set_exit_code()

    def set_exit_code(self):
        for exe in self.execs_:
            if exe.exit_code:
                self.exit_code = exe.exit_code


class PsshExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.PSSH, **kwargs)

