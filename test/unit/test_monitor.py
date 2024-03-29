from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.util.hostfile import Hostfile
from jarvis_util.introspect.system_info import Lsblk, \
    ListFses, FiInfo, Blkid, ResourceGraph, StorageDeviceType
from jarvis_util.util.size_conv import SizeConv
import pathlib
import itertools
import os
from unittest import TestCase
from jarvis_util.introspect.monitor import Monitor, MonitorParser

class TestSystemInfo(TestCase):

    def test_monitor_parser(self):
        parser = MonitorParser(os.path.join(os.environ['HOME'], 'monitor'))
        parser.parse()
        avg = parser.avg_memory()
        print(avg)
