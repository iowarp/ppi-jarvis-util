"""
This module provides methods for querying the information of the host
system. This can be used to make scripts more portable.
"""

import socket
import re
import platform
from jarvis_util.shell.exec import Exec
from jarvis_util.util.size_conv import SizeConv
from jarvis_util.serialize.yaml_file import YamlFile
import jarvis_util.util.small_df as sdf
import json
import yaml
import shlex
import ipaddress
import copy
# pylint: disable=C0121


# Note, not using enum to avoid YAML serialization errors
# YAML expects simple types
class StorageDeviceType:
    PMEM = 'pmem'
    NVME = 'nvme'
    SSD = 'ssd'
    HDD = 'hdd'


class SystemInfo:
    """
    This class queries information about the host machine, such as OS,
    CPU, and kernel
    """

    instance_ = None

    @staticmethod
    def get_instance():
        if SystemInfo.instance_ is None:
            SystemInfo.instance_ = SystemInfo()
        return SystemInfo.instance_

    def __init__(self):
        with open('/etc/os-release', 'r', encoding='utf-8') as fp:
            lines = fp.read().splitlines()
            self.os = self._detect_os_type(lines)
            self.os_like = self._detect_os_like_type(lines)
            self.os_version = self._detect_os_version(lines)
        self.ksemantic = platform.platform()
        self.krelease = platform.release()
        self.ktype = platform.system()
        self.cpu = platform.processor()
        self.cpu_family = platform.machine()

    def _detect_os_type(self, lines):
        for line in lines:
            if 'ID=' in line:
                if 'ubuntu' in line:
                    return 'ubuntu'
                elif 'centos' in line:
                    return 'centos'
                elif 'debian' in line:
                    return 'debian'

    def _detect_os_like_type(self, lines):
        for line in lines:
            if 'ID_LIKE=' in line:
                if 'ubuntu' in line:
                    return 'ubuntu'
                elif 'centos' in line:
                    return 'centos'
                elif 'debian' in line:
                    return 'debian'

    def _detect_os_version(self, lines):
        for line in lines:
            grp = re.match('VERSION_ID=\"(.*)\"', line)
            if grp:
                return grp.group(1)

    def __hash__(self):
        return hash(str([self.os, self.os_like,
                         self.os_version, self.ksemantic,
                         self.krelease, self.ktype,
                         self.cpu, self.cpu_family]))

    def __eq__(self, other):
        return (
            (self.os == other.os) and
            (self.os_like == other.os_like) and
            (self.os_version == other.os_version) and
            (self.ksemantic == other.ksemantic) and
            (self.krelease == other.krelease) and
            (self.cpu == other.cpu) and
            (self.cpu_family == other.cpu_family)
        )


class Lsblk(Exec):
    """
    List all block devices in the system per-node. Lsblk will return
    a JSON output

    A table is stored per-host:
        parent: the parent device of the partition (e.g., /dev/sda or NaN)
        device: the name of the partition (e.g., /dev/sda1)
        size: total size of the partition (bytes)
        mount: where the partition is mounted (if anywhere)
        model: the exact model of the device
        tran: the transport of the device (e.g., /dev/nvme)
        rota: whether or not the device is rotational
        dev_type: the category of the device
        host: the host this record corresponds to
    """

    def __init__(self, exec_info):
        cmd = 'lsblk -o NAME,SIZE,MODEL,TRAN,MOUNTPOINT,ROTA -J'
        super().__init__(cmd, exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.df = None
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        total = []
        for host, stdout in self.stdout.items():
            lsblk_data = json.loads(stdout)['blockdevices']
            if len(lsblk_data) == 0:
                continue
            for dev in lsblk_data:
                parent = f'/dev/{dev["name"]}'
                if dev['size'] is None:
                    dev['size'] = '0'
                if dev['tran'] is None:
                    dev['tran'] = 'sata'
                if dev['rota'] is None:
                    dev['rota'] = False
                total.append({
                    'parent': None,
                    'device': parent,
                    'size': SizeConv.to_int(dev['size']),
                    'model': dev['model'],
                    'tran': dev['tran'].lower(),
                    'mount': dev['mountpoint'],
                    'rota': dev['rota'],
                    'dev_type': self.GetDevType(dev),
                    'host': host
                })
                if 'children' not in dev:
                    continue
                for partition in dev['children']:
                    if partition['size'] is None:
                        partition['size'] = '0'
                    total.append({
                        'parent': parent,
                        'device': f'/dev/{partition["name"]}',
                        'size': SizeConv.to_int(partition['size']),
                        'model': dev['model'],
                        'tran': dev['tran'].lower(),
                        'mount': partition['mountpoint'],
                        'rota': dev['rota'],
                        'dev_type': self.GetDevType(dev),
                        'host': host
                    })
        self.df = sdf.SmallDf(rows=total)
        print(self.df)

    def GetDevType(self, dev):
        if dev['tran'] == 'sata':
            if dev['rota']:
                return str(StorageDeviceType.HDD)
            else:
                return str(StorageDeviceType.SSD)
        elif dev['tran'] == 'nvme':
            return str(StorageDeviceType.NVME)
        elif dev['tran'] == 'dimm':
            return str(StorageDeviceType.PMEM)


class PyLsblk(Exec):
    """
       List all block devices in the system per-node. PyLsblk will return
       a YAML output

       A table is stored per-host:
           parent: the parent device of the partition (e.g., /dev/sda or NaN)
           device: the name of the partition (e.g., /dev/sda1)
           size: total size of the partition (bytes)
           mount: where the partition is mounted (if anywhere)
           model: the exact model of the device
           tran: the transport of the device (e.g., /dev/nvme)
           rota: whether or not the device is rotational
           host: the host this record corresponds to
       """

    def __init__(self, exec_info):
        cmd = 'pylsblk'
        super().__init__(cmd, exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.df = None
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        total = []
        for host, stdout in self.stdout.items():
            lsblk_data = yaml.load(stdout, Loader=yaml.FullLoader)
            for dev in lsblk_data:
                if dev['tran'] == 'pcie':
                    dev['tran'] = 'nvme'
                dev['host'] = host
                total.append(dev)
        self.df = sdf.SmallDf(rows=total)
        print(self.df)


class Blkid(Exec):
    """
    List all filesystems (even those unmounted) and their properties

    Stores a per-host table with the following:
        device: the device (or partition) which stores the data (e.g., /dev/sda)
        fs_type: the type of filesystem (e.g., ext4)
        uuid: filesystem-level uuid from the FS metadata
        partuuid: the partition-lable UUID for the partition
        partlabel: semantic partition type information
        label: semantic label given by users
        host: the host this entry corresponds to
    """
    def __init__(self, exec_info):
        cmd = 'blkid'
        super().__init__(cmd, exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.df = None
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        dev_list = []
        for host, stdout in self.stdout.items():
            devices = stdout.splitlines()
            for dev in devices:
                dev_dict = {}
                toks = shlex.split(dev)
                dev_name = toks[0].split(':')[0]
                dev_dict['device'] = dev_name
                dev_dict['host'] = host
                for tok in toks[1:]:
                    keyval = tok.split('=')
                    key = keyval[0].lower()
                    val = ' '.join(keyval[1:])
                    dev_dict[key] = val
                dev_list.append(dev_dict)
        df = sdf.SmallDf(dev_list)
        df = df.rename({'type': 'fs_type'})
        self.df = df


class ListFses(Exec):
    """
    List all mounted filesystems

    Will store a per-host dictionary containing:
        device: the device which contains the filesystem
        fs_size: total size of the filesystem
        used: total nubmer of bytes used
        avail: total number of bytes remaining
        use%: the percent of capacity used
        fs_mount: where the filesystem is mounted
        host: the host this entry corresponds to
    """

    def __init__(self, exec_info):
        cmd = 'df -h'
        super().__init__(cmd, exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.df = None
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        columns = ['device', 'fs_size', 'used',
                   'avail', 'use%', 'fs_mount', 'host']
        rows = []
        for host, stdout in self.stdout.items():
            lines = stdout.strip().splitlines()
            rows += [line.split() + [host] for line in lines[1:]]
        df = sdf.SmallDf(rows, columns=columns)
        self.df = df


class FiInfo(Exec):
    """
    List all networks and their information
        provider: network protocol (e.g., sockets, tcp, ib)
        fabric: IP address
        domain: network domain
        version: network version
        type: packet type (e.g., DGRAM)
        protocol: protocol constant
        host: the host this network corresponds to
    """
    def __init__(self, exec_info):
        super().__init__('fi_info', exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.graph = {}
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        providers = []
        for host, stdout in self.stdout.items():
            lines = stdout.strip().splitlines()
            for line in lines:
                if 'provider' in line:
                    providers.append({
                        'provider': line.split(':')[1].strip(),
                        'host': host
                    })
                else:
                    splits = line.split(':')
                    key = splits[0].strip()
                    val = splits[1].strip()
                    providers[-1][key] = val
        self.df = sdf.SmallDf(providers)
        self.df.drop_duplicates()


class ResourceGraph:
    """
    Stores helpful information about storage and networking info for machines.

    Two tables are stored to make decisions on application deployment.
    fs:
        parent: the parent device of the partition (e.g., /dev/sda or NaN)
        device: the name of the device (e.g., /dev/sda1 or /dev/sda)
        mount: where the device is mounted (if anywhere)
        model: the exact model of the device
        dev_type: type of device
        fs_type: the type of filesystem (e.g., ext4)
        uuid: filesystem-levle uuid from the FS metadata
        avail: total number of bytes remaining
        shared: whether the this is a shared service or not
        host: the host this record corresponds to
    net:
        provider: network protocol (e.g., sockets, tcp, ib)
        fabric: IP address
        domain: network domain
        host: the host this network corresponds to
        speed: the network speed of the interconnect
        shared: whether the network is used across hosts

    TODO: Need to verify on more than ubuntu20.04
    TODO: Can we make this work for windows?
    TODO: Can we make this work even when hosts have different OSes?
    """

    def __init__(self):
        self.lsblk = None
        self.blkid = None
        self.list_fs = None
        self.fi_info = None
        self.fs_columns = [
            'parent', 'device', 'mount', 'model', 'dev_type',
            'fs_type', 'uuid',
            'avail', 'shared', 'host',
        ]
        self.net_columns = [
            'provider', 'fabric', 'domain', 'host',
            'speed', 'shared'
        ]
        self.create()
        self.hosts = None
        self.path = None

    """
    Build the resource graph
    """

    def create(self):
        self.fs = sdf.SmallDf(columns=self.fs_columns)
        self.net = sdf.SmallDf(columns=self.net_columns)

    def build(self, exec_info, introspect=True):
        """
        Build a resource graph.

        :param exec_info: Where to collect resource information
        :param introspect: Whether to pylsblk system info, or rely solely
        on admin-defined settings
        :return: self
        """
        self.create()
        if introspect:
            self._introspect(exec_info)
        self.apply()
        return self

    def walkthrough_build(self, exec_info, introspect=True):
        self.build(exec_info, introspect)
        self.walkthrough_prune(exec_info)

    def walkthrough_prune(self, exec_info):
        print('(2/4). Finding mount points common across machines')
        mounts = self.find_storage(common=True, condense=True)
        self.print_df(mounts)
        # Add missing mountpoints
        x = self._ask_yes_no('2.(1/3). Are there any mount points missing '
                             'you would like to add?',
                             default='no')
        new_devs = []
        while x:
            mount = self._ask_string('2.1.(1/6). Mount point')
            mount = mount.replace(r'\$', '$')
            dev_type = self._ask_choices('2.1.(2/6). What transport?',
                                     choices=['hdd', 'ssd', 'nvme', 'pmem'])
            shared = self._ask_yes_no('2.1.(3/6). Is this device shared? '
                                      'I.e., a PFS?')
            avail = self._ask_size('2.1.(4/6). How much capacity are you '
                                   'willing to use?')
            y = self._ask_yes_no('2.1.(5/6). Are you sure this is accurate?',
                                 default='yes')
            if not y:
                continue
            new_devs.append({
                'mount': mount,
                'dev_type': dev_type,
                'shared': shared,
                'avail': avail,
                'size': avail,
            })
            x = self._ask_yes_no('2.1.(6/6). Are there any other '
                                 'devices you would like to add?',
                                 default='no')
            if x is None:
                x = False
        self.add_storage(exec_info.hostfile, new_devs)
        # Correct discovered mount points
        x = self._ask_yes_no('2.(2/3). Would you like to correct '
                             'any mountpoints?',
                             default='no')
        while x:
            regex = self._ask_re('2.2.(1/3). Enter a regex of mount '
                                 'points to select',
                                 default='.*').strip()
            if regex is None:
                regex = '.*'
            if regex.endswith('*'):
                regex = f'^{regex}'
            else:
                regex = f'^{regex}$'
            matches = mounts[lambda r: re.match(regex, r['mount']), 'mount']
            print(matches.to_string())
            y = self._ask_yes_no('Is this correct?', default='yes')
            if not y:
                continue
            suffix = self._ask_string('2.2.(2/3). Enter a suffix to '
                                      'append to these paths.',
                                      default='${USER}')
            y = self._ask_yes_no('Are you sure this is accurate?',
                                 default='yes')
            if not y:
                continue
            suffix = suffix.replace(r'\$', '$')
            self.add_suffix(regex, mount_suffix=suffix)
            x = self._ask_yes_no('2.2.(3/3). Do you want to select more '
                                 'mount points?',
                                 default='no')
        # Eliminate unneded mount points
        x = self._ask_yes_no(
            '2.(3/3). Would you like to remove any mount points?',
            default='no')
        mounts = self.fs['mount'].unique().list()
        print(f'Mount points: {mounts}')
        while x:
            regex = self._ask_re('2.3.(1/3). Enter a regex of mount '
                                 'points to remove.').strip()
            if regex is None:
                regex = '.*'
            if regex.endswith('*'):
                regex = f'^{regex}'
            else:
                regex = f'^{regex}$'
            matches = self.fs[lambda r: re.match(regex, r['mount']), 'mount']
            print(matches.to_string())
            y = self._ask_yes_no('2.3.(2/3). Is this correct?', default='yes')
            if not y:
                continue
            self.fs = self.fs[lambda r: not re.match(regex, r['mount'])]
            mounts = self.fs['mount'].unique().list()
            print(f'Mount points: {mounts}')
            x = self._ask_yes_no('2.3.(3/3). Any more?', default='no')

        # Fill in missing network information
        print('(3/4). Finding network info')
        net_info = self.find_net_info(exec_info.hostfile)
        fabrics = net_info['fabric'].unique().list()
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        print(f'This IP addr of this node ({hostname}) is: {ip_address}')
        print(f'We detect the following {len(fabrics)} fabrics: {fabrics}')
        x = False
        # pylint: disable=W0640
        for fabric in fabrics:
            fabric_info = net_info[lambda r: r['fabric'] == fabric]
            print(f'3.(1/4). Now modifying {fabric}:')
            print(f'Providers: {fabric_info["provider"].unique().list()}')
            print(f'Domains: {fabric_info["domain"].unique().list()}')
            x = self._ask_yes_no('3.(2/4). Keep this fabric?', default='no')
            if not x:
                net_info = net_info[lambda r: r['fabric'] != fabric]
                print()
                continue
            shared = self._ask_yes_no('3.(3/4). '
                                      'Is this fabric shared across hosts?',
                                      default='yes')
            speed = self._ask_size('3.(4/4). '
                                   'What is the speed of this network?')
            fabric_info['speed'] = speed
            fabric_info['shared'] = shared
            print()
        # pylint: enable=W0640
        self.net = net_info
        x = self._ask_yes_no('(4/4). Are all hosts symmetrical? I.e., '
                             'the per-node resource graphs should all '
                             'be the same.',
                             default='yes')
        if x:
            self.make_common(exec_info.hostfile)
        self.apply()

    def _ask_string(self, msg, default=None):
        if default is None:
            x = input(f'{msg}: ')
        else:
            x = input(f'{msg} (Default: {default}): ')
        if len(x) == 0:
            x = default
        return x

    def _ask_re(self, msg, default=None):
        if default is not None:
            msg = f'{msg} (Default: {default})'
        x = input(f'{msg}: ')
        if len(x) == 0:
            x = default
        if x is None:
            x = ''
        return x

    def _ask_yes_no(self, msg, default=None):
        while True:
            msg = f'{msg} (yes/no)'
            if default is not None:
                msg = f'{msg} (Default: {default})'
            x = input(f'{msg}: ')
            if x == '':
                x = default
            if x == 'yes':
                return True
            elif x == 'no':
                return False
            else:
                print(f'{x} is not either yes or no')

    def _ask_choices(self, msg, choices):
        choices_str = '/'.join(choices)
        while True:
            x = input(f'{msg} ({choices_str}): ')
            if x in choices:
                return x
            else:
                print(f'{x} is not a valid choice')

    def _ask_size(self, msg):
        x = input(f'{msg} (kK,mM,gG,tT,pP): ')
        size = SizeConv.to_int(x)
        return size

    def _introspect(self, exec_info):
        """
        Introspect the cluster for resources.

        :param exec_info: Where to collect resource information
        :return: None
        """
        self.lsblk = PyLsblk(exec_info.mod(hide_output=True))
        self.blkid = Blkid(exec_info.mod(hide_output=True))
        self.list_fs = ListFses(exec_info.mod(hide_output=True))
        self.fi_info = FiInfo(exec_info.mod(hide_output=True))
        self.hosts = exec_info.hostfile.hosts
        self.fs = sdf.merge([self.fs, self.lsblk.df, self.blkid.df],
                            on=['device', 'host'],
                            how='outer')
        self.fs[:, 'shared'] = False
        self.fs = sdf.merge([self.fs, self.list_fs.df],
                               on=['device', 'host'],
                               how='outer')
        self.fs.drop_columns([
            'used', 'use%', 'fs_mount', 'partuuid', 'fs_size',
            'partlabel', 'label'])
        net_df = self.fi_info.df
        net_df[:, 'speed'] = 0
        net_df.drop_columns(['version', 'type', 'protocol'])
        net_df.drop_duplicates()
        self.net = net_df

    def save(self, path):
        """
        Save the resource graph YAML file

        :param path: the path to save the file
        :return: None
        """
        graph = {
            'hosts': self.hosts,
            'fs': self.fs.rows,
            'net': self.net.rows,
        }
        YamlFile(path).save(graph)
        self.path = path

    def load(self, path):
        """
        Load resource graph from storage.

        :param path: The path to the resource graph YAML file
        :return: self
        """
        graph = YamlFile(path).load()
        self.path = path
        self.hosts = graph['hosts']
        self.fs = sdf.SmallDf(graph['fs'], columns=self.fs_columns)
        self.net = sdf.SmallDf(graph['net'], columns=self.net_columns)
        self.apply()
        return self

    def _derive_storage_cols(self):
        df = self.fs
        if df is None or len(df) == 0:
            return
        df['mount'].fillna('')
        df['shared'].fillna(True)
        df['tran'].fillna('')
        df['size'].fillna(0)
        df['size'].apply(lambda r, c: SizeConv.to_int(r[c]))
        noavail = df[lambda r: r['avail'] == 0 or r['avail'] is None, :]
        noavail['avail'] = noavail['size']
        df['avail'].apply(lambda r, c: SizeConv.to_int(r[c]))

    def _derive_net_cols(self):
        self.net['domain'].fillna('')

    def set_hosts(self, hosts):
        """
        Set the set of hosts this resource graph covers

        :param hosts: Hostfile()
        :return: None
        """
        self.hosts = hosts.hosts_ip

    """
    Update the resource graph
    """

    def add_storage(self, hosts, records):
        """
        Register a set of storage devices on a set of hosts

        :param hosts: Hostfile() indicating set of hosts to make record for
        :param records: A list or single dict of device info
        :return: None
        """
        if not isinstance(records, list):
            records = [records]
        new_rows = []
        for host in hosts.hosts:
            for record in records:
                record = copy.deepcopy(record)
                record['host'] = host
                new_rows.append(record)
        new_df = sdf.SmallDf(rows=new_rows, columns=self.fs.columns)
        self.fs = sdf.concat([self.fs, new_df])
        self.apply()

    def add_net(self, hosts, records):
        """
        Register a network record

        :param hosts: Hostfile() indicating set of hosts to make record for
        :param records: A list or single dict of network info
        :return: None
        """
        new_rows = []
        new_rows = []
        for host, ip in zip(hosts.hosts, hosts.hosts_ip):
            for record in records:
                record = copy.deepcopy(record)
                record['fabric'] = ip
                record['host'] = host
                new_rows.append(record)
        new_df = sdf.SmallDf(rows=new_rows, columns=self.net.columns)
        self.net = sdf.concat([self.net, new_df])
        self.apply()

    def filter_fs(self, mount_res):
        """
        Track all filesystems + devices matching the mount regex.

        :param mount_res: A list or single regex to match mountpoints
        :return: self
        """
        self.fs = self.find_storage(mount_res=mount_res)
        self.apply()
        return self

    def add_suffix(self, mount_re, mount_suffix):
        """
        Track all filesystems + devices matching the mount regex.

        :param mount_re: The regex to match a set of mountpoints
        :param mount_suffix: After the mount_re is matched, append this path
        to the mountpoint to indicate where users can access data. A typical
        value for this is /${USER}, indicating the mountpoint has a subdirectory
        per-user where they can access data.
        :return: self
        """
        df = self.fs[lambda r: re.match(mount_re, str(r['mount'])), 'mount']
        df += f'/{mount_suffix}'
        return self

    def make_common(self, hosts):
        """
        This resource graph should contain only entries common across all hosts.

        :return: self
        """
        self.fs = self.find_storage(common=True,
                                    condense=True)
        self.net = self.find_net_info(hosts, condense=True)
        return self

    def apply(self):
        """
        Remove duplicates from the host
        :return:
        """
        self.fs.drop_duplicates()
        self.net.drop_duplicates()
        self._derive_net_cols()
        self._derive_storage_cols()

    """
    Query the resource graph
    """

    def find_shared_storage(self):
        """
        Find the set of shared storage services

        :return: Dataframe
        """
        df = self.fs
        return df[lambda r: r['shared'] == True]

    def find_storage(self,
                     dev_types=None,
                     is_mounted=True,
                     common=False,
                     condense=False,
                     count_per_node=None,
                     count_per_dev=None,
                     min_cap=None,
                     min_avail=None,
                     mount_res=None,
                     shared=None,
                     df=None):
        """
        Find a set of storage devices.

        :param dev_types: Search for devices of type in order. Either a list
        or a string.
        :param is_mounted: Search only for mounted devices
        :param common: Remove mount points that are not common across all hosts
        :param condense: Used in conjunction with common. Will remove the 'host'
        column and will only contain one entry per mount point.
        :param count_per_node: Choose only a subset of devices matching query
        :param count_per_dev: Choose only a subset of devices matching query
        :param min_cap: Remove devices with too little overall capacity
        :param min_avail: Remove devices with too little available space
        :param mount_res: A regex or list of regexes to match mount points
        :param shared: Whether to search for devices which are shared
        :param df: The data frame to run this query
        :return: Dataframe
        """
        if df is None:
            df = self.fs
        # Filter devices by whether or not a mount is needed
        if is_mounted:
            df = df[lambda r: r['mount'] != '']
        # Filter devices matching the mount regex
        if mount_res is not None:
            if not isinstance(mount_res, (list, tuple, set)):
                mount_res = [mount_res]
            df = df[lambda r:
                    any(re.match(reg, str(r['mount'])) for reg in mount_res)]
        # Find devices of a particular type
        if dev_types is not None:
            if not isinstance(dev_types, (list, tuple, set)):
                dev_types = [dev_types]
            df = df[lambda r: str(r['dev_type']) in dev_types]
        # Get the set of mounts common between all hosts
        if common:
            df = df.groupby(['mount']).filter_groups(
                lambda x: len(x) == len(self.hosts)).reset_index()
        # Remove storage with too little capacity
        if min_cap is not None:
            df = df[lambda r: r['size'] >= min_cap]
        # Remove storage with too little available space
        if min_avail is not None:
            df = df[lambda r: r['avail'] >= min_avail]
        # Take a certain number of each device per-host
        if count_per_dev is not None:
            df = df.groupby(['dev_type', 'host']).\
                head(count_per_dev).reset_index()
        # Take a certain number of matched devices per-host
        if count_per_node is not None:
            df = df.groupby('host').head(count_per_node).reset_index()
        if common and condense:
            df = df.groupby(['mount']).first().reset_index()
        #     df = df.drop_columns('host')
        if shared is not None:
            df = df[lambda r: r['shared'] == shared]
        return df

    @staticmethod
    def _subnet_matches_hosts(subnet, ip_addrs):
        # pylint: disable=W0702
        try:
            network = ipaddress.ip_network(subnet, strict=False)
        except:
            return True
        # pylint: enable=W0702
        for ip in ip_addrs:
            if ip in network:
                return True
        return False

    def find_net_info(self,
                      hosts=None,
                      strip_ips=False,
                      providers=None,
                      condense=False,
                      shared=None,
                      df=None):
        """
        Find the set of networks common between each host.

        :param hosts: A Hostfile() data structure containing the set of
        all hosts to find network information for
        :param strip_ips: remove IPs that are not compatible with the hostfile
        :param providers: The network protocols to search for.
        :param condense: Only retain information for a single host
        :param df: The df to use for this query
        :param shared: Filter out local networks
        :return: Dataframe
        """
        if df is None:
            df = self.net
        if hosts is not None:
            # Get the set of fabrics corresponding to these hosts
            if strip_ips:
                ips = [ipaddress.ip_address(ip) for ip in hosts.hosts_ip]
                df = df[lambda r: self._subnet_matches_hosts(r['fabric'], ips)]
            # Filter out protocols which are not common between these hosts
            if condense:
                grp = df.groupby(['provider', 'domain']).filter_groups(
                   lambda x: len(x) >= len(hosts))
                df = grp.first().reset_index()
        # Choose only a subset of providers
        if providers is not None:
            if not isinstance(providers, (list, set)):
                providers = [providers]
            providers = set(providers)
            df = df[lambda r: r['provider'] in providers]
        # Choose only shared networks
        if shared is not None:
            if shared:
                df = df[lambda r: r['shared']]
            else:
                df = df[lambda r: not r['shared']]
        return df

    def print_df(self, df):
        if 'device' in df.columns:
            if 'host' in df.columns:
                col = ['host', 'mount', 'device', 'dev_type', 'shared',
                       'avail', 'tran', 'rota', 'fs_type']
                df = df[col]
                df = df.sort_values('host')
                print(df.to_string())
            else:
                col = ['device', 'mount', 'dev_type', 'shared',
                       'avail', 'tran', 'rota', 'fs_type']
                df = df[col]
                print(df.to_string())
        else:
            print(df.sort_values('provider').to_string())


# pylint: enable=C0121
