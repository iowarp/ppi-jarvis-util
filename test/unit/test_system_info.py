from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.util.hostfile import Hostfile
from jarvis_util.introspect.system_info import Lsblk, \
    ListFses, FiInfo, Blkid, ResourceGraph
import pathlib
from unittest import TestCase


class TestSystemInfo(TestCase):
    def test_lsblk(self):
        Lsblk(LocalExecInfo(hide_output=True))

    def test_list_fses(self):
        ListFses(LocalExecInfo(hide_output=True))

    def test_fi_info(self):
        FiInfo(LocalExecInfo(hide_output=True))

    def test_blkid(self):
        Blkid(LocalExecInfo(hide_output=True))

    def test_resource_graph(self):
        rg = ResourceGraph()
        rg.build(LocalExecInfo(hide_output=True))
        rg.save('/tmp/resource_graph.yaml')
        rg.load('/tmp/resource_graph.yaml')
        rg.filter_fs(r'/$', '/${USER}', 'NVME')
        rg.filter_hosts(Hostfile(), '1gbps')
        rg.save('/tmp/resource_graph.yaml')
