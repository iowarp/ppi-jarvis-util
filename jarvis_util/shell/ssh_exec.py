from pssh.clients import ParallelSSHClient

class SSHExec(ParallelNode):
    def __init__(self, cmds,
                 user=None, pkey=None, password=None, port=None, sudo=False):
        self.stdout = {}
        self.stderr = {}

        #Make sure commands are a list
        if isinstance(cmds, list):
            self.cmds=cmds
        elif isinstance(cmds, str):
            self.cmds=[cmds]
        else:
            raise Error(ErrorCode.INVALID_TYPE).format("SSHExecNode cmds", type(cmds))

        # Convert list into a single command
        cmd = self._group_cmds()

        # Send command over SSH
        self._exec_ssh(cmd)

    def _group_cmds(self):
        if self.exec_async:
            for i, cmd in enumerate(self.cmds):
                self.cmds[i] += '  > /dev/null 2>&1 &'
        if self.sudo:
            self.cmds.insert(0, f"source /home/{self.username}/.bashrc")
        cmd = " ; ".join(self.cmds)
        return cmd

    def _exec_ssh(self, cmd):
        client = ParallelSSHClient(self.hosts,
                                   user=self.username,
                                   pkey=self.pkey,
                                   password=self.password,
                                   port=self.port)
        output = client.run_command(cmd, sudo=self.sudo)
        for host_output in output:
            host = host_output.host
            self.stdout[host] = "\n".join(list(host_output.stdout))
            self.stderr[host] = "\n".join(list(host_output.stderr))

    def __str__(self):
        return "SSHExec {}".format(self.name)
