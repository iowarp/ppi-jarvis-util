class SshExec:
    def __init__(self, cmds,
                 user=None, pkey=None, port=None, sudo=False,
                 exec_async=False, collect_output=True):
        self.user = user
        self.pkey = pkey
        self.port = port
        self.sudo = sudo
