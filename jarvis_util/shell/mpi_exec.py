"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""

from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec
from .exec_info import ExecInfo, ExecType
from abc import abstractmethod


class MpiVersion(LocalExec):
    """
    Introspect the current MPI implementation from the machine using
    mpirun --version
    """

    def __init__(self, exec_info):
        self.cmd = 'mpiexec --version'
        super().__init__(self.cmd,
                         exec_info.mod(env=exec_info.basic_env,
                                       collect_output=True,
                                       hide_output=True))
        vinfo = self.stdout
        # print(f'MPI INFO: stdout={vinfo} stderr={self.stderr}')
        if 'mpich' in vinfo.lower():
            self.version = ExecType.MPICH
        elif 'Open MPI' in vinfo or 'OpenRTE' in vinfo:
            self.version = ExecType.OPENMPI
        elif 'Intel(R) MPI Library' in vinfo:
            # NOTE(llogan): similar to MPICH
            self.version = ExecType.INTEL_MPI
        elif 'mpiexec version' in vinfo:
            self.version = ExecType.CRAY_MPICH
        else:
            raise Exception(f'Could not identify MPI implementation: {vinfo}')


class LocalMpiExec(LocalExec):
    def __init__(self, cmd, exec_info):
        self.cmd = cmd
        self.nprocs = exec_info.nprocs
        self.ppn = exec_info.ppn
        self.hostfile = exec_info.hostfile
        self.mpi_env = exec_info.env
        if exec_info.do_dbg:
            self.base_cmd = cmd # To append to the extra processes
            self.cmd = self.get_dbg_cmd(cmd, exec_info.dbg_port)
        super().__init__(self.mpicmd(),
                         exec_info.mod(env=exec_info.basic_env,
                                       do_dbg=False))

    @abstractmethod
    def mpicmd(self):
        pass

class OpenMpiExec(LocalMpiExec):
    """
    This class contains methods for executing a command in parallel
    using MPI.
    """
    def mpicmd(self):
        params = [f'mpiexec -n {self.nprocs}']
        params.append('--oversubscribe')
        if self.ppn is not None:
            params.append(f'-npernode {self.ppn}')
        if len(self.hostfile):
            if self.hostfile.is_subset() or self.hostfile.path is None:
                params.append(f'--host {",".join(self.hostfile.hosts)}')
            else:
                params.append(f'--hostfile {self.hostfile.path}')
        params += [f'-x {key}=\"{val}\"'
                   for key, val in self.mpi_env.items()]
        params.append(self.cmd)
        cmd = ' '.join(params)
        jutil = JutilManager.get_instance()
        if jutil.debug_mpi_exec:
            print(cmd)
        return cmd


class MpichExec(LocalMpiExec):
    """
    This class contains methods for executing a command in parallel
    using MPI.
    """

    def mpicmd(self):
        params = ['mpiexec']

        if self.ppn is not None:
            params.append(f'-ppn {self.ppn}')

        if len(self.hostfile):
            if self.hostfile.is_subset() or self.hostfile.path is None:
                params.append(f'--host {",".join(self.hostfile.hosts)}')
            else:
                params.append(f'--hostfile {self.hostfile.path}')

        params += [f'-genv {key}=\"{val}\"'
                   for key, val in self.mpi_env.items()]

        if self.cmd.startswith('gdbserver'):
            params.append(f'-n 1 {self.cmd}')
            if self.nprocs > 1:
                params.append(f': -n {self.nprocs - 1} {self.base_cmd}')
        else:
            params.append(f'-n {self.nprocs}')
            params.append(self.cmd)

        cmd = ' '.join(params)

        jutil = JutilManager.get_instance()
        if jutil.debug_mpi_exec:
            print(cmd)

        return cmd

class CrayMpichExec(LocalMpiExec):
    """
    This class contains methods for executing a command in parallel
    using MPI.
    """
    def mpicmd(self):
        params = [f'mpiexec -n {self.nprocs}']
        if self.ppn is not None:
            params.append(f'--ppn {self.ppn}')
        if len(self.hostfile):
            if self.hostfile.hosts[0] == 'localhost' and len(self.hostfile) == 1:
                pass
            elif self.hostfile.is_subset() or self.hostfile.path is None:
                params.append(f'--hosts {",".join(self.hostfile.hosts)}')
            else:
                params.append(f'--hostfile {self.hostfile.path}')
        params += [f'--env {key}=\"{val}\"'
                   for key, val in self.mpi_env.items()]
        params.append(self.cmd)
        cmd = ' '.join(params)
        jutil = JutilManager.get_instance()
        if jutil.debug_mpi_exec:
            print(cmd)
        return cmd

class MpiExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.MPI, **kwargs)
