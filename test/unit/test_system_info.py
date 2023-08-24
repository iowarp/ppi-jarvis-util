from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.util.hostfile import Hostfile
from jarvis_util.introspect.system_info import Lsblk, \
    ListFses, FiInfo, Blkid, ResourceGraph, StorageDeviceType
from jarvis_util.util.size_conv import SizeConv
import pathlib
import itertools
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
        rg.filter_fs(r'/$')
        rg.add_suffix(r'/$', '/${USER}')
        rg.save('/tmp/resource_graph.yaml')

    def test_custom_resource_graph(self):
        rg = ResourceGraph()
        all_hosts = ['host1', 'host2', 'host3']
        all_hosts_ip = ['192.168.1.0', '192.168.1.1', '192.168.1.2']
        providers = ['tcp', 'ib', 'roce']
        hosts = Hostfile(all_hosts=all_hosts, all_hosts_ip=all_hosts_ip)

        # Add networks for each node
        rg.set_hosts(hosts)
        rg.add_net(hosts,
                   [{'provider': provider} for provider in providers])
        rg.add_net(hosts.subset(1),
                   [{'provider': 'uncommon'}])

        # Two HDD, SSD, and NVME per-host
        # 2 * 3 * 3 = 18 devices
        rg.add_storage(hosts, [
            {
                'device': '/dev/sda1',
                'mount': '/',
                'tran': 'sata',
                'rota': True,
                'size': SizeConv.to_int('10g'),
                'shared': False
            },
            {
                'device': '/dev/sda2',
                'mount': '/mnt/hdd/$USER',
                'tran': 'sata',
                'rota': True,
                'size': SizeConv.to_int('200g'),
                'shared': False
            },
            {
                'device': '/dev/sdb1',
                'mount': '/mnt/ssd/$USER',
                'tran': 'sata',
                'rota': False,
                'size': SizeConv.to_int('50g'),
                'shared': False
            },
            {
                'device': '/dev/sdb2',
                'mount': '/mnt/ssd2/$USER',
                'tran': 'sata',
                'rota': False,
                'size': SizeConv.to_int('50g'),
                'shared': False
            },
            {
                'device': '/dev/nvme0n1',
                'mount': '/mnt/nvme/$USER',
                'tran': 'nvme',
                'rota': False,
                'size': SizeConv.to_int('100g'),
                'shared': False
            },
            {
                'device': '/dev/nvme0n3',
                'mount': '/mnt/nvme3/$USER',
                'tran': 'nvme',
                'rota': False,
                'size': SizeConv.to_int('100g'),
                'shared': False
            }
        ])
        self.assertEqual(18, len(rg.fs))
        # One NVMe on first host
        # 19 devices
        rg.add_storage(hosts.subset(1), [
            {
                'device': '/dev/nvme0n2',
                'mount': '/mnt/nvme2/$USER',
                'tran': 'nvme',
                'rota': False,
                'size': SizeConv.to_int('10g'),
                'shared': False
            }
        ])
        self.assertEqual(19, len(rg.fs))

        # Filter only mounts in '/mnt'
        rg.filter_fs('/mnt/*')
        self.assertEqual(16, len(rg.fs))

        # Find all mounted NVMes
        df = rg.find_storage(dev_types=[StorageDeviceType.NVME])
        self.assertTrue(len(df[lambda r: r['tran'] == 'nvme']) == 7)
        self.assertTrue(len(df[lambda r: r['tran'] == 'sata']) == 0)
        self.assertTrue(len(df) == 7)

        # Find all mounted & common NVMes and SSDs
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True)
        self.assertTrue(len(df[lambda r: r['tran'] == 'nvme']) == 6)
        self.assertTrue(len(df[lambda r: r['tran'] == 'sata']) == 6)
        self.assertTrue(len(df) == 12)

        # Select a single nvme and ssd per-node
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True,
                             count_per_dev=1)
        self.assertTrue(len(df[lambda r: r['tran'] == 'nvme']) == 3)
        self.assertTrue(len(df[lambda r: r['tran'] == 'sata']) == 3)
        self.assertTrue(len(df) == 6)

        # Get condensed output
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True,
                             condense=True,
                             count_per_dev=1)
        self.assertTrue(len(df[lambda r: r['tran'] == 'nvme']) == 2)
        self.assertTrue(len(df[lambda r: r['tran'] == 'sata']) == 2)
        self.assertTrue(len(df) == 4)
        rg.print_df(df)

        # Find common networks between hosts
        df = rg.find_net_info(hosts)
        self.assertTrue(len(df) == 9)

        # Find common tcp networks
        df = rg.find_net_info(hosts, providers='tcp')
        self.assertTrue(len(df) == 3)

        # Find common + condensed TCP networks
        df = rg.find_net_info(hosts, providers='tcp', condense=True)
        self.assertTrue(len(df) == 1)

        rg.print_df(df)
