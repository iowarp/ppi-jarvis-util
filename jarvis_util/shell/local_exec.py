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
        self.file_output = exec_info.file_output
        if self.collect_output is None:
            self.collect_output = jutil.collect_output
        if self.file_output is not None:
            self.file_output = open(self.file_output, 'a')
        if self.hide_output is None:
            self.hide_output = jutil.hide_output
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()
        self.executing_ = True
        self.print_thread = None
        print(f"CMD: {cmd}")

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

    def print_to_outputs(self, out, sysout):
        text = out.read().decode('utf-8')
        print(f"TEXT: {len(text)}")
        if not self.hide_output:
            sysout.write(text)
        if self.collect_output:
            self.stdout.write(text)
        if self.file_output is not None:
            self.file_output.write(text)

    def print_worker(self):
        while self.executing_:
            self.print_to_outputs(self.proc.stdout, sys.stdout)
            self.print_to_outputs(self.proc.stderr, sys.stderr)
            time.sleep(25 / 1000)

    def join_print_worker(self):
        if self.print_thread is None:
            return
        self.executing_ = False
        self.print_thread.join()
        if self.file_output is not None:
            self.file_output.close()
        self.stdout = self.stdout.getvalue()
        self.stderr = self.stderr.getvalue()

    def kill(self):
        if self.proc is not None:
            LocalExec(f"kill -9 {self.get_pid()}",
                      ExecInfo(collect_output=False))
            self.proc.kill()
            self.join_print_worker()

    def wait(self):
        self.proc.wait()
        self.join_print_worker()
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
