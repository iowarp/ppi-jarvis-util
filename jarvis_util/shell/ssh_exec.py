from .local_exec import LocalExec


class SshExec(LocalExec):
    def __init__(self, cmd,
                 user=None, pkey=None, port=None, sudo=False, env=None,
                 exec_async=False, collect_output=True):
        self.user = user
        self.pkey = pkey
        self.port = port
        self.sudo = sudo
        self.env = env
        super().__init__(self.ssh_cmd(cmd),
                         exec_async=exec_async,
                         collect_output=collect_output)

    def ssh_cmd(self, cmd):
        lines = ['ssh']
        if self.pkey is not None:
            lines.append(f"-i {self.pkey}")
        if self.port is not None:
            lines.append(f"-p {self.port}")
        if self.env is not None:
            for key, val in self.env.items():
                lines.append(f"{key}={val}")
        lines.append(cmd)
        return " ".join(lines)
