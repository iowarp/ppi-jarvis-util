from enum import Enum
import copy


class ExecType(Enum):
    LOCAL = 'LOCAL'
    SSH = 'SSH'
    PSSH = 'PSSH'
    MPI = 'MPI'


class ExecInfo:
    def __init__(self,  exec_type=ExecType.LOCAL, nprocs=None, ppn=None,
                 user=None, pkey=None, port=None, hostfile=None, env=None,
                 sleep_ms=0, sudo=False, cwd=None, hosts=None,
                 collect_output=False, exec_async=False, stdin=None):
        self.exec_type = exec_type
        self.nprocs = nprocs
        self.user = user
        self.pkey = pkey
        self.port = port
        self.ppn = ppn
        self.hostfile = hostfile
        self.hosts = hosts
        self.env = env
        self.cwd = cwd
        self.sudo = sudo
        self.sleep_ms = sleep_ms
        self.collect_output = collect_output
        self.exec_async = exec_async
        self.stdin = stdin

    def mod(self, **kwargs):
        cpy = copy.deepcopy(self)
        for key, val in kwargs.items():
            setattr(cpy, key, val)
        return cpy

    def copy(self):
        return self.mod()
