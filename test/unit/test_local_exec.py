
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo

ret = LocalExec("echo hello", LocalExecInfo(collect_output=False))
ret = LocalExec("echo hello", LocalExecInfo(collect_output=True))
ret = LocalExec("echo hello", LocalExecInfo(collect_output=True,
                                            hide_output=True))
ret = LocalExec("echo hello", LocalExecInfo(collect_output=True,
                                            file_output='/tmp/test.log',
                                            hide_output=True))
assert(str(ret.stdout).strip() == "hello")
