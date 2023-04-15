from .local_exec import LocalExec
from .exec_info import ExecInfo, ExecType


class SshExec(LocalExec):
    def __init__(self, cmd, exec_info):
        self.addr = exec_info.hosts[0]
        self.user = exec_info.user
        self.pkey = exec_info.pkey
        self.port = exec_info.port
        self.sudo = exec_info.sudo
        self.env = exec_info.env
        super().__init__(self.ssh_cmd(cmd), exec_info)

    def ssh_cmd(self, cmd):
        lines = ['ssh']
        if self.pkey is not None:
            lines.append(f"-i {self.pkey}")
        if self.port is not None:
            lines.append(f"-p {self.port}")
        if self.user is not None:
            lines.append(f"{self.user}@{self.addr}")
        else:
            lines.append(f"{self.addr}")
        if self.env is not None:
            for key, val in self.env.items():
                lines.append(f"{key}={val}")
        lines.append(cmd)
        return " ".join(lines)


class SshExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.SSH, **kwargs)
