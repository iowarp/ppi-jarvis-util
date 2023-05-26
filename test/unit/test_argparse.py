from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
import pathlib
from unittest import TestCase


class TestArgparse(TestCase):

    def set_paths(self):
        HERE = str(pathlib.Path(__file__).parent.resolve())
        self.path = f'{HERE}/argparse_main.py'
        self.cmd = f'python3 {self.path}'

    def getcmd(self, *args):
        argstr = ' '.join(args)
        return f'{self.cmd} {argstr}'

    def test_help(self):
        node = Exec(self.getcmd(),
                    LocalExecInfo(collect_output=True))

