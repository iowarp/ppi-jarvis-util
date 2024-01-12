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
                   [{'provider': provider, 'shared': True}
                    for provider in providers])
        rg.add_net(hosts.subset(1),
                   [{'provider': 'uncommon'}])

        # Two HDD, SSD, and NVME per-host
        # 2 * 3 * 3 = 18 devices
        rg.add_storage(hosts, [
            {
                'device': '/dev/sda1',
                'mount': '/',
                'dev_type': 'hdd',
                'size': '10g',
                'shared': False
            },
            {
                'device': '/dev/sda2',
                'mount': '/mnt/hdd/$USER',
                'dev_type': 'hdd',
                'size': '200g',
                'shared': False
            },
            {
                'device': '/dev/sdb1',
                'mount': '/mnt/ssd/$USER',
                'dev_type': 'ssd',
                'size': '50g',
                'shared': False
            },
            {
                'device': '/dev/sdb2',
                'mount': '/mnt/ssd2/$USER',
                'dev_type': 'ssd',
                'size': '50g',
                'shared': False
            },
            {
                'device': '/dev/nvme0n1',
                'mount': '/mnt/nvme/$USER',
                'dev_type': 'nvme',
                'size': '100g',
                'shared': False
            },
            {
                'device': '/dev/nvme0n3',
                'mount': '/mnt/nvme3/$USER',
                'dev_type': 'nvme',
                'size': '100g',
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
                'dev_type': 'nvme',
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
        self.assertEqual(7, len(df[lambda r: r['dev_type'] == 'nvme']))
        self.assertEqual(0, len(df[lambda r: r['dev_type'] == 'hdd']))
        self.assertEqual(0, len(df[lambda r: r['dev_type'] == 'ssd']))
        self.assertEqual(7, len(df))

        # Find all mounted & common NVMes and SSDs
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True)
        self.assertEqual(6, len(df[lambda r: r['dev_type'] == 'nvme']))
        self.assertEqual(6, len(df[lambda r: r['dev_type'] == 'ssd']))
        self.assertEqual(12, len(df))

        # Select a single nvme and ssd per-node
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True,
                             count_per_dev=1)
        self.assertEqual(3, len(df[lambda r: r['dev_type'] == 'nvme']))
        self.assertEqual(3, len(df[lambda r: r['dev_type'] == 'ssd']))
        self.assertEqual(6, len(df))

        # Get condensed output
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True,
                             condense=True,
                             count_per_dev=1)
        self.assertEqual(1, len(df[lambda r: str(r['dev_type']) == 'nvme']))
        self.assertEqual(1, len(df[lambda r: str(r['dev_type']) == 'ssd']))
        self.assertEqual(2, len(df))
        rg.print_df(df)

        # Find common networks between hosts
        df = rg.find_net_info(hosts, shared=True)
        self.assertEqual(9, len(df))

        # Find common tcp networks
        df = rg.find_net_info(hosts, providers='tcp')
        self.assertEqual(3, len(df))

        # Find common + condensed TCP networks
        df = rg.find_net_info(hosts, providers='tcp', condense=True)
        self.assertEqual(1, len(df))

        rg.print_df(df)

    def test_add_suffix(self):
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

        # Add storage
        rg.add_storage(hosts, [
            {
                'device': '/dev/sda1',
                'mount': '/',
                'dev_type': 'ssd',
                'size': SizeConv.to_int('10g'),
                'shared': False
            }
        ])

        rg.add_suffix('/', '${USER}')
        df = rg.find_storage(mount_res=r'.*\${USER}')
        self.assertEqual(3, len(df))

    def test_ares(self):
        rg = ResourceGraph()
        TEST_DIR = pathlib.Path(__file__).parent.resolve()
        rg = rg.load(f'{TEST_DIR}/ares.yaml')
        hosts = Hostfile()
        rg.make_common(hosts)