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
        self.exec_async = exec_info.exec_async
        self.hosts = exec_info.hostfile.hosts
        self.execs_ = []
        self.stdout = {}
        self.stderr = {}
        self.is_local = exec_info.hostfile.is_local()
        if not self.is_local:
            dbg_cmd = cmd
            if exec_info.do_dbg:
                dbg_cmd = self.get_dbg_cmd(cmd, exec_info.dbg_port)
            for i, host in enumerate(self.hosts):
                sshcmd = cmd
                if i == 0:
                    sshcmd = dbg_cmd
                ssh_exec_info = exec_info.mod(hostfile=None,
                                              hosts=host,
                                              exec_async=True,
                                              do_dbg=False)
                self.execs_.append(SshExec(sshcmd, ssh_exec_info))
        else:
            self.execs_.append(
                LocalExec(cmd, exec_info.mod(exec_async=True)))
        if not self.exec_async:
            self.wait()

    def wait(self):
        self.wait_list(self.execs_)
        if not self.is_local:
            self.per_host_outputs(self.execs_)
        else:
            self.stdout = {'localhost': self.execs_[0].stdout}
            self.stderr = {'localhost': self.execs_[0].stderr}
        self.set_exit_code()

    def set_exit_code(self):
        self.set_exit_code_list(self.execs_)


class PsshExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.PSSH, **kwargs)

