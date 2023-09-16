from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.shell.mpi_exec import MpiVersion
from jarvis_util.util.size_conv import SizeConv
import pathlib
import itertools
from unittest import TestCase


class TestSystemInfo(TestCase):
    def test_mpi(self):
        info = MpiVersion(LocalExecInfo())
        print(f'MPI VERSION: {info.version}')
