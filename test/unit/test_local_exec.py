
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo

#ret = LocalExec("echo hello", LocalExecInfo(collect_output=False))
ret = LocalExec("echo hello", LocalExecInfo(collect_output=True))
assert(str(ret.stdout).strip() == "hello")
