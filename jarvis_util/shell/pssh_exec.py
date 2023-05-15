"""
This module provides methods to distribute a command among multiple
nodes using SSH. This class is intended to be called from Exec,
not by general users.
"""

from .ssh_exec import SshExec
from .local_exec import LocalExec
from .exec_info import ExecInfo, ExecType, Executable


class PsshExec(Executable):
    """
    Execute commands on multiple hosts using SSH.
    """

    def __init__(self, cmd, exec_info):
        """
        Execute commands on multiple hosts.

        :param cmd: A list of commands or a single command string
        :param exec_info: Info needed to execute command with SSH
        """
        super().__init__()
        self.cmd = self.smash_cmd(cmd)
        self.exec_async = exec_info.exec_async
        self.hosts = exec_info.hostfile.hosts
        self.execs_ = []
        self.stdout = {}
        self.stderr = {}
        if len(self.hosts):
            for host in self.hosts:
                ssh_exec_info = exec_info.mod(hostfile=None,
                                              hosts=host,
                                              exec_async=True)
                self.execs_.append(SshExec(cmd, ssh_exec_info))
        else:
            self.execs_.append(
                LocalExec(cmd, exec_info))
            return
        if not self.exec_async:
            self.wait()

    def wait(self):
        for exe in self.execs_:
            exe.wait()
            if hasattr(exe, 'addr'):
                addr = exe.addr
            else:
                addr = 'localhost'
            self.stdout[addr] = exe.stdout
            self.stdout[addr] = exe.stderr
        self.set_exit_code()

    def set_exit_code(self):
        for exe in self.execs_:
            if exe.exit_code:
                self.exit_code = exe.exit_code


class PsshExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.PSSH, **kwargs)

