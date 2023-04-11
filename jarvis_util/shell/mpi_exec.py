from jarvis_util.shell.local_exec import LocalExec


class MpiExec(LocalExec):
    def __init__(self, cmd, nprocs=1, ppn=None, env=None):
        self.nprocs = nprocs
        self.ppn = ppn
        self.cmd = cmd
        self.env = env
        if env is None:
            self.env = {}
        super().__init__(self.mpicmd())

    def mpicmd(self):
        params = [f"mpirun -n {self.nprocs}"]
        if self.ppn is not None:
            params.append(f"-ppn {self.ppn}")
        params += [f"-genv {key}={val}" for key, val in self.env.items()]
        params.append(self.cmd)
        cmd = " ".join(params)
        return cmd
