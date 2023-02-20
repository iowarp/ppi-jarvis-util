import time


class LocalExec:
    def __init__(self, cmd,
                 sudo=False,
                 collect_output=True,
                 cwd=None,
                 stdin=None,
                 do_async=False,
                 sleep_ms=0):
        self.cmd = cmd
        self.sudo = sudo
        self.stdin = stdin
        self.do_async = do_async
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
                                         shell=True)
        else:
            self.proc = subprocess.Popen(self.cmd,
                                         stdin=self.stdin,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         cwd=self.cwd,
                                         shell=True)
        self.stdout, self.stderr = self.proc.communicate()
        self.stdout = self.stdout.decode("utf-8")
        self.stderr = self.stderr.decode("utf-8")
        self.exit_code = self.proc.returncode

    def kill(self):
        if self.proc is not None:
            LocalExecNode(f"kill -9 {self.GetPid()}", collect_output=False)
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