from jarvis_util.util.hostfile import Hostfile
import pathlib
from unittest import TestCase


class TestHostfile(TestCase):
    def test_no_expand_int(self):
        host = Hostfile()
        host.parse('0')
        self.assertTrue(len(host.hosts) == 1)
        self.assertTrue(host.hosts[0] == '0')

    def test_no_expand(self):
        host = Hostfile()
        host.parse('ares-comp-01')
        self.assertTrue(len(host.hosts) == 1)
        self.assertTrue(host.hosts[0] == 'ares-comp-01')

    def test_expand_set(self):
        host = Hostfile()
        host.parse('ares-comp-[01-04]-40g')
        self.assertTrue(len(host.hosts) == 4)
        self.assertTrue(host.hosts[0] == 'ares-comp-01-40g')
        self.assertTrue(host.hosts[1] == 'ares-comp-02-40g')
        self.assertTrue(host.hosts[2] == 'ares-comp-03-40g')
        self.assertTrue(host.hosts[3] == 'ares-comp-04-40g')

    def test_expand_two_sets(self):
        host = Hostfile()
        host.parse('ares-comp-[01-02]-40g-[01-02]')
        self.assertTrue(len(host.hosts) == 4)
        self.assertTrue(host.hosts[0] == 'ares-comp-01-40g-01')
        self.assertTrue(host.hosts[1] == 'ares-comp-01-40g-02')
        self.assertTrue(host.hosts[2] == 'ares-comp-02-40g-01')
        self.assertTrue(host.hosts[3] == 'ares-comp-02-40g-02')

    def test_subset(self):
        host = Hostfile()
        host.parse('ares-comp-[01-02]-40g-[01-02]')
        host = host.subset(3)
        self.assertTrue(len(host.hosts) == 3)
        self.assertTrue(host.is_subset())
        self.assertTrue(host.hosts[0] == 'ares-comp-01-40g-01')
        self.assertTrue(host.hosts[1] == 'ares-comp-01-40g-02')
        self.assertTrue(host.hosts[2] == 'ares-comp-02-40g-01')

    def test_read_hostfile(self):
        HERE = str(pathlib.Path(__file__).parent.resolve())
        host = Hostfile(hostfile=f"{HERE}/test_hostfile.txt")
        print(host.hosts)
        self.assertTrue(len(host.hosts) == 15)
