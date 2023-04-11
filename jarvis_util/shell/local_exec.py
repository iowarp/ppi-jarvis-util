import time
import subprocess
import os
from jarvis_util.jutil_manager import JutilManager


class LocalExec:
    def __init__(self, cmd,
                 sudo=False,
                 collect_output=None,
                 cwd=None,
                 env=None,
                 stdin=None,
                 exec_async=False,
                 sleep_ms=0):
        jutil = JutilManager.get_instance()
        if collect_output is None:
            collect_output = jutil.collect_output
        self.cmd = cmd
        self.sudo = sudo
        self.env = env
        self.stdin = stdin
        self.exec_async = exec_async
        self.sleep_ms = sleep_ms
        self.collect_output = collect_output
        if cwd is None:
            self.cwd = os.getcwd()
        else:
            self.cwd = cwd
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
        self.stdout, self.stderr = self.proc.communicate()
        self.stdout = self.stdout.decode("utf-8")
        self.stderr = self.stderr.decode("utf-8")
        self.exit_code = self.proc.returncode
        if not self.exec_async:
            self.wait()

    def kill(self):
        if self.proc is not None:
            LocalExec(f"kill -9 {self.get_pid()}", collect_output=False)
            self.proc.kill()

    def wait(self):
        self.stdout, self.stderr = self.proc.communicate()
        self.stdout = self.stdout.decode("utf-8")
        self.stderr = self.stderr.decode("utf-8")
        self.exit_code = self.proc.returncode
        self.proc.wait()

    def get_pid(self):
        if self.proc is not None:
            return self.proc.pid
        else:
            return None