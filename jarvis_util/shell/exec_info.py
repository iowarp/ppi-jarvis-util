from enum import Enum
from jarvis_util.util.hostfile import Hostfile
import copy
from abc import ABC, abstractmethod


class ExecType(Enum):
    LOCAL = 'LOCAL'
    SSH = 'SSH'
    PSSH = 'PSSH'
    MPI = 'MPI'


class ExecInfo:
    def __init__(self,  exec_type=ExecType.LOCAL, nprocs=None, ppn=None,
                 user=None, pkey=None, port=None, hostfile=None, env=None,
                 sleep_ms=0, sudo=False, cwd=None, hosts=None,
                 collect_output=False, hide_output=False, file_output=None,
                 exec_async=False, stdin=None):
        self.exec_type = exec_type
        self.nprocs = nprocs
        self.user = user
        self.pkey = pkey
        self.port = port
        self.ppn = ppn
        self.hostfile = hostfile
        if hostfile is not None:
            if isinstance(hostfile, str):
                self.hostfile = Hostfile(hostfile=hostfile)
            elif isinstance(hostfile, Hostfile):
                self.hostfile = hostfile
            else:
                raise Exception("Hostfile is neither string nor Hostfile")
        if hosts is not None:
            if isinstance(hosts, list):
                self.hostfile = Hostfile(hosts=hostfile)
            elif isinstance(hosts, Hostfile):
                self.hostfile = hosts
        if hosts is not None and hostfile is not None:
            raise Exception("Must choose either hosts or hostfile, not both")
        self.env = env
        self.cwd = cwd
        self.sudo = sudo
        self.sleep_ms = sleep_ms
        self.collect_output = collect_output
        self.file_output = file_output
        self.hide_output = hide_output
        self.exec_async = exec_async
        self.stdin = stdin

    def mod(self, **kwargs):
        cpy = copy.deepcopy(self)
        for key, val in kwargs.items():
            setattr(cpy, key, val)
        return cpy

    def copy(self):
        return self.mod()


class Executable:
    def __init__(self):
        self.exit_code = None

    @abstractmethod
    def set_exit_code(self):
        pass

    @abstractmethod
    def wait(self):
        pass

