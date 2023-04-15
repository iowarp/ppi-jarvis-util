from .ssh_exec import SshExec
from .local_exec import LocalExec
from jarvis_util.util.hostfile import Hostfile


class PsshExec:
    def __init__(self, cmd,
                 hostfile=None,
                 collect_output=True,
                 exec_async=False,
                 env=None):
        self.cmd = cmd
        self.exec_async = exec_async
        self.hosts = []
        self.execs_ = []
        if hostfile is not None:
            self.hosts = Hostfile(hostfile).hosts

        if len(self.hosts):
            for host in self.hosts:
                self.execs_.append(
                    SshExec(cmd, host,
                            exec_async=True,
                            collect_output=collect_output,
                            env=env))
        else:
            self.execs_.append(
                LocalExec(cmd,
                          exec_async=exec_async,
                          collect_output=collect_output,
                          env=env))
            return
        if not self.exec_async:
            self.wait()

    def wait(self):
        for exe in self.execs_:
            exe.wait()
