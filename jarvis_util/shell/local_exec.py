import time
import subprocess
import os
from jarvis_util.jutil_manager import JutilManager
from .exec_info import ExecInfo, ExecType, Executable


class LocalExec(Executable):
    def __init__(self, cmd, exec_info):
        super().__init__()
        jutil = JutilManager.get_instance()
        self.collect_output = exec_info.collect_output
        if self.collect_output is None:
            self.collect_output = jutil.collect_output
        self.cmd = cmd
        self.sudo = exec_info.sudo
        env = exec_info.env
        if env is None:
            env = {}
        for key, val in os.environ.items():
            if key not in env:
                env[key] = val
        self.env = env
        self.stdin = exec_info.stdin
        self.exec_async = exec_info.exec_async
        self.sleep_ms = exec_info.sleep_ms
        self.collect_output = exec_info.collect_output
        if exec_info.cwd is None:
            self.cwd = os.getcwd()
        else:
            self.cwd = exec_info.cwd
        self.stdout = None
        self.stderr = None
        self._start_bash_processes()

    def _start_bash_processes(self):
        if self.sudo:
            self.cmd = f"sudo {self.cmd}"
        time.sleep(self.sleep_ms)
        if not self.collect_output:
            self.proc = subprocess.Popen(self.cmd,
                                         stdin=self.stdin,
                                         cwd=self.cwd,
                                         env=self.env,
                                         shell=True)
        else:
            self.proc = subprocess.Popen(self.cmd,
                                         stdin=self.stdin,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         cwd=self.cwd,
                                         env=self.env,
                                         shell=True)
        if not self.exec_async:
            self.wait()

    def kill(self):
        if self.proc is not None:
            LocalExec(f"kill -9 {self.get_pid()}",
                      ExecInfo(collect_output=False))
            self.proc.kill()

    def wait(self):
        self.stdout, self.stderr = self.proc.communicate()
        if self.collect_output:
            self.stdout = self.stdout.decode("utf-8")
            self.stderr = self.stderr.decode("utf-8")
        self.proc.wait()
        self.set_exit_code()
        return self.exit_code

    def set_exit_code(self):
        self.exit_code = self.proc.returncode

    def get_pid(self):
        if self.proc is not None:
            return self.proc.pid
        else:
            return None


class LocalExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.LOCAL, **kwargs)
