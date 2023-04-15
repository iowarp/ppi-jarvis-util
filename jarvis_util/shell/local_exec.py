import time
import subprocess
import os, sys
import io
import threading
from jarvis_util.jutil_manager import JutilManager
from .exec_info import ExecInfo, ExecType, Executable


class LocalExec(Executable):
    def __init__(self, cmd, exec_info):
        super().__init__()
        jutil = JutilManager.get_instance()

        # Managing console output and collection
        self.collect_output = exec_info.collect_output
        self.hide_output = exec_info.hide_output
        if self.collect_output is None:
            self.collect_output = jutil.collect_output
        if self.hide_output is None:
            self.collect_output = jutil.hide_output
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()
        self.executing_ = True
        self.print_thread = None

        # Managing command execution
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
        if exec_info.cwd is None:
            self.cwd = os.getcwd()
        else:
            self.cwd = exec_info.cwd
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
            self.print_thread = threading.Thread(target=self.print_worker)
            self.print_thread.start()
        if not self.exec_async:
            self.wait()

    def print_worker(self):
        while self.executing_:
            for line in self.proc.stdout:
                line = line.decode('utf-8')
                if not self.hide_output:
                    sys.stdout.write(line)
                self.stdout.write(line)
            for line in self.proc.stderr:
                line = line.decode('utf-8')
                if not self.hide_output:
                    sys.stderr.write(line)
                self.stderr.write(line)

    def kill(self):
        if self.proc is not None:
            LocalExec(f"kill -9 {self.get_pid()}",
                      ExecInfo(collect_output=False))
            self.proc.kill()
            self.executing_ = False
            if self.print_thread is not None:
                self.print_thread.join()

    def wait(self):
        self.proc.wait()
        self.executing_ = False
        if self.print_thread is not None:
            self.print_thread.join()
        self.stdout = self.stdout.getvalue()
        self.stderr = self.stderr.getvalue()
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
