
from jarvis_util.shell.local_exec import LocalExec

ret = LocalExec("echo hello")
assert(str(ret.stdout).strip() == "hello")
