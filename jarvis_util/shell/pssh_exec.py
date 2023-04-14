from .ssh_exec import SshExec
from jarvis_util.util.hostfile import Hostfile


class PsshExec:
    def __init__(self, cmd, exec_info,
                 collect_output=True, exec_async=False):
        self.cmd = cmd
        self.exec_info = exec_info
        self.exec_async = exec_async
        self.hosts = []
        if exec_info.hostfile is None:
            self.hosts = Hostfile(exec_info.hostfile).hosts

        self.execs_ = []
        for host in self.hosts:
            self.execs_.append(SshExec(cmd, host,
                                       exec_async=True,
                                       collect_output=collect_output))
        if not self.exec_async:
            self.wait()

    def wait(self):
        for exec in self.execs_:
            exec.wait()
