# PPI Jarvis Util

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![IoWarp](https://img.shields.io/badge/IoWarp-GitHub-blue.svg)](http://github.com/iowarp)
[![GRC](https://img.shields.io/badge/GRC-Website-blue.svg)](https://grc.iit.edu/)
[![Python](https://img.shields.io/badge/Python-3.7+-yellow.svg)](https://www.python.org/)

A Python library containing various utilities to aid with creating shell scripts within Python. This library provides wrappers for executing shell commands locally, SSH, SCP, MPI, argument parsing, and various other utilities.

## Purpose
PPI Jarvis Util simplifies the creation of shell scripts within Python by providing comprehensive wrappers for command execution, remote operations, and system management tasks. It enables seamless integration between Python applications and system-level operations.

## Dependencies

### Python Dependencies
- `pyyaml` - YAML parsing and generation
- `tabulate` - Pretty-print tabular data

## Installation

For now, we only consider manual installation
```bash
cd jarvis-util
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
```

## Executing a program

The following code will execute a command on the local machine.
The output will NOT be collected into an in-memory buffer.
The output will be printed to the terminal as it occurs.

```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo 

node = Exec('echo hello', LocalExecInfo(collect_output=False))
```

Programs can also be executed asynchronously:
```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo 

node = Exec('echo hello', LocalExecInfo(collect_output=False,
                                        exec_async=True))
node.wait()
```

Various examples of outputs:
```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo 

# Will ONLY print to the terminal
node = Exec('echo hello', LocalExecInfo(collect_output=False))
# Will collect AND print to the terminal
node = Exec('echo hello', LocalExecInfo(collect_output=True))
# Will collect BUT NOT print to the terminal
node = Exec('echo hello', LocalExecInfo(collect_output=True,
                                        hide_output=True))
# Will collect, pipe to file, and print to terminal
node = Exec('echo hello', LocalExecInfo(collect_output=True,
                                        pipe_stdout='/tmp/stdout.txt',
                                        pipe_stderr='/tmp/stderr.txt'))
```

This is useful if you have a program which runs using a daemon mode.

## Executing an MPI program

The following code will execute the "hostname" command on the local
machine 24 times using MPI.

```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.mpi_exec import MpiExecInfo 

node = Exec('hostname', MpiExecInfo(hostfile=None,
                                    nprocs=24,
                                    ppn=None,
                                    collect_output=False))
```

## Executing an SSH program

The following code will execute the "hostname" command on all machines
in the hostfile

```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.pssh_exec import PsshExecInfo 

node = Exec('hostname', PsshExecInfo(hostfile="/tmp/hostfile.txt",
                                     collect_output=False))
```

## The contents of a hostfile

A hostfile can have the following syntax:
```
ares-comp-01
ares-comp-[02-04]
ares-comp-[05-09,11,12-14]-40g
```

These will be expanded internally by PSSH and MPI.

## Project Structure

- `bin/` - Command-line utilities (pylsblk, pymonitor)
- `jarvis_util/` - Core Python library
  - `shell/` - Command execution wrappers (local, SSH, MPI, etc.)
  - `util/` - Utility modules (argparse, hostfile, logging, etc.)
  - `serialize/` - File serialization modules (YAML, JSON, INI, etc.)
  - `introspect/` - System monitoring and information modules
- `example/` - Usage examples and demonstrations
- `test/unit/` - Unit tests
- `ci/` - CI/CD configuration and Docker setup

# Contributing

We use the Google Python Style guide (pylintrc).

## License

BSD 3-Clause License

Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology. See the [LICENSE](LICENSE) file for details.

## Support

For issues, questions, or contributions, please:
- Open an issue on the [GitHub repository](https://github.com/iowarp/ppi-jarvis-util)
- Contact the Gnosis Research Center at Illinois Institute of Technology
