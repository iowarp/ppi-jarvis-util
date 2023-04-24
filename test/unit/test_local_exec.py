import pathlib
import os
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo


ret = LocalExec("echo hello", LocalExecInfo())
assert(str(ret.stdout).strip() != "hello")

ret = LocalExec("echo hello", LocalExecInfo())
ret = LocalExec("echo hello", LocalExecInfo(hide_output=True))
ret = LocalExec("echo hello", LocalExecInfo(pipe_stdout='/tmp/test.log',
                                            hide_output=True,
                                            collect_output=True))
assert(str(ret.stdout).strip() == "hello")

HERE = str(pathlib.Path(__file__).parent.resolve())
PRINT10s = os.path.join(HERE, 'print10s.py')
ret = LocalExec(f"python3 {PRINT10s}", LocalExecInfo())
