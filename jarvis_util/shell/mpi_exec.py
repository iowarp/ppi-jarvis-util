from jarvis_util.shell.local_exec import LocalExec


class MpiExec(LocalExec):
    def __init__(self, cmd,
                 nprocs=1, ppn=None, hostfile=None,
                 exec_async=False, collect_output=None, env=None):
        self.nprocs = nprocs
        self.ppn = ppn
        self.cmd = cmd
        self.hostfile = hostfile
        self.env = env
        if env is None:
            self.env = {}
        super().__init__(self.mpicmd(),
                         exec_async=exec_async,
                         collect_output=collect_output)

    def mpicmd(self):
        params = [f"mpirun -n {self.nprocs}"]
        if self.ppn is not None:
            params.append(f"-ppn {self.ppn}")
        if self.hostfile is not None:
            params.append(f"--hostfile {self.hostfile}")
        params += [f"-genv {key}={val}" for key, val in self.env.items()]
        params.append(self.cmd)
        cmd = " ".join(params)
        return cmd
