"""
This module provides methods for querying the information of the host
system. This can be used to make scripts more portable.
"""

import re
import platform
from jarvis_util.shell.exec import Exec
from jarvis_util.util.size_conv import SizeConv
from jarvis_util.serialize.yaml_file import YamlFile
import jarvis_util.util.small_df as sdf
import json
import shlex
import ipaddress
# pylint: disable=C0121


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
        host: the host this record corresponds to
    """

    def __init__(self, exec_info):
        cmd = 'lsblk -o NAME,SIZE,MODEL,TRAN,MOUNTPOINT,ROTA -J -s'
        super().__init__(cmd, exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.df = None
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        partitions = []
        devs = []
        for host, stdout in self.stdout.items():
            lsblk_data = json.loads(stdout)['blockdevices']
            if len(lsblk_data) == 0:
                continue
            for partition in lsblk_data:
                dev = partition
                if 'children' in partition:
                    dev = partition['children'][0]
                partitions.append({
                    'parent': f'/dev/{dev["name"]}',
                    'device': f'/dev/{partition["name"]}',
                    'size': SizeConv.to_int(partition['size']),
                    'mount': partition['mountpoint'],
                    'host': host
                })
                devs.append({
                    'parent': f'/dev/{dev["name"]}',
                    'size': SizeConv.to_int(dev['size']),
                    'model': dev['model'],
                    'tran': dev['tran'].lower()
                    if dev['tran'] is not None else '',
                    'mount': dev['mountpoint'],
                    'rota': dev['rota'],
                    'host': host
                })
        part_df = sdf.SmallDf(rows=partitions)
        dev_df = sdf.SmallDf(rows=devs)
        total_df = sdf.merge(
            part_df,
            dev_df[['parent', 'model', 'tran', 'host']],
            on=['parent', 'host'])
        dev_df = dev_df.rename({'parent': 'device'})
        total_df = sdf.concat([total_df, dev_df])
        self.df = total_df


class Blkid(Exec):
    """
    List all filesystems (even those unmounted) and their properties

    Stores a per-host table with the following:
        device: the device (or partition) which stores the data (e.g., /dev/sda)
        fs_type: the type of filesystem (e.g., ext4)
        uuid: filesystem-levle uuid from the FS metadata
        partuuid: the partition-lable UUID for the partition
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
        # pylint: disable=W0108
        df.loc[:, 'fs_size'] = df['fs_size'].apply(
            lambda x: SizeConv.to_int(x))
        df.loc[:, 'used'] = df['used'].apply(
            lambda x: SizeConv.to_int(x))
        df.loc[:, 'avail'] = df['avail'].apply(
            lambda x: SizeConv.to_int(x))
        # pylint: enable=W0108
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


# Note, not using enum to avoid YAML serialization errors
# YAML expects simple types
class StorageDeviceType:
    PMEM = 'pmem'
    NVME = 'nvme'
    SSD = 'ssd'
    HDD = 'hdd'


class ResourceGraph:
    """
    Stores helpful information about storage and networking info for machines.

    Two tables are stored to make decisions on application deployment.
    fs:
        parent: the parent device of the partition (e.g., /dev/sda or NaN)
        device: the name of the device (e.g., /dev/sda1 or /dev/sda)
        size: total size of the device (bytes)
        mount: where the device is mounted (if anywhere)
        model: the exact model of the device
        rota: whether the device is rotational or not
        tran: the transport of the device (e.g., /dev/nvme)
        dev_type: type of device (derviced from rota + tran)
        fs_type: the type of filesystem (e.g., ext4)
        uuid: filesystem-levle uuid from the FS metadata
        fs_size: total size of the filesystem
        avail: total number of bytes remaining
        shared: whether the this is a shared service or not
        host: the host this record corresponds to
    net:
        provider: network protocol (e.g., sockets, tcp, ib)
        fabric: IP address
        domain: network domain
        host: the host this network corresponds to
        speed: the network speed of the interconnect

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
            'parent', 'device', 'size', 'mount', 'model', 'rota',
            'tran', 'fs_type', 'uuid', 'fs_size',
            'avail', 'shared', 'host'
        ]
        self.net_columns = [
            'provider', 'fabric', 'domain', 'host', 'speed'
        ]
        self.create()
        self.hosts = None

    def create(self):
        self.all_fs = sdf.SmallDf(columns=self.fs_columns)
        self.all_net = sdf.SmallDf(columns=self.net_columns)
        self.fs = None
        self.net = None
        self.fs_settings = {
            'register': [],
            'filter_mounts': {}
        }
        self.net_settings = {
            'register': [],
            'track_ips': {}
        }

    def build(self, exec_info, introspect=True):
        """
        Build a resource graph.

        :param exec_info: Where to collect resource information
        :param introspect: Whether to introspect system info, or rely solely
        on admin-defined settings
        :return: self
        """
        self.create()
        if introspect:
            self._introspect(exec_info)
        self.apply()
        return self

    def walkthrough_build(self, exec_info):
        """
        This provides a CLI to build the resource graph

        :return: None
        """
        print('This is the Jarivs resource graph builder')
        print('(1/3). Introspecting your machine')
        self.build(exec_info)
        print('(2/3). Finding mount points common across machines')
        mounts = self.find_storage(common=True, condense=True)
        self.print_df(mounts)
        x = self._ask_yes_no('2.(1/2). Are there any mount points missing '
                             'you would like to add?',
                             default='no')
        while x:
            mount = self._ask_string('2.1.(1/7). Mount point')
            tran = self._ask_choices('2.1.(2/7). What transport?',
                                     choices=['sata', 'nvme', 'dimm'])
            rota = self._ask_yes_no('2.1.(3/7). Is this device rotational. '
                                    'I.e., is it a hard drive?')
            shared = self._ask_yes_no('2.1.(4/7). Is this device shared? '
                                      'I.e., a PFS?')
            avail = self._ask_size('2.1.(5/7). How much capacity are you '
                                   'willing to use?')
            y = self._ask_yes_no('2.1.(6/7). Are you sure this is accurate?',
                                 default='yes')
            if not y:
                continue
            self.add_storage(exec_info.hostfile, mount=mount,
                             tran=tran, rota=rota, shared=shared,
                             avail=avail)
            x = self._ask_yes_no('2.1.(7/7). Registered. Are there any other '
                                 'devices you would like to add?',
                                 default='no')
            if x is None:
                x = False
        print('2.(2/2). Filter and correct mount points.')
        x = True
        while x:
            regex = self._ask_re('2.2.(1/3). Enter a regex of mount '
                                 'points to select. Default: .*').strip()
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
                                      'append to these paths. '
                                      'Hit enter for no suffix.')
            y = self._ask_yes_no('Are you sure this is accurate?')
            if not y:
                continue
            self.filter_fs(regex, mount_suffix=suffix)
            x = self._ask_yes_no('2.2.(3/3). Do you want to select more '
                                 'mount points?')
        x = print('(3/3). Listing networks.')
        if x:
            net_info = self.find_net_info(exec_info.hostfile)
            self.print_df(net_info)

    def _ask_string(self, msg):
        x = input(f'{msg}: ')
        return x

    def _ask_re(self, msg):
        x = input(f'{msg}. E.g., * selects everything, /mnt/* for everything '
                  f'prefixed with /mnt: ')
        return x

    def _ask_yes_no(self, msg, default=None):
        while True:
            txt = [
                f'{msg} (yes/no)',
                f' (Default: {default})' if default is not None else ''
            ]
            txt = ''.join(txt)
            x = input(f'{txt}: ')
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
        self.lsblk = Lsblk(exec_info.mod(hide_output=True))
        self.blkid = Blkid(exec_info.mod(hide_output=True))
        self.list_fs = ListFses(exec_info.mod(hide_output=True))
        self.fi_info = FiInfo(exec_info.mod(hide_output=True))
        self.hosts = exec_info.hostfile.hosts
        self.all_fs = sdf.merge(self.lsblk.df,
                                self.blkid.df,
                                on=['device', 'host'],
                                how='outer')
        self.all_fs.loc[:, 'shared'] = False
        self.all_fs = sdf.merge(self.all_fs,
                               self.list_fs.df,
                               on=['device', 'host'],
                               how='outer')
        self.all_fs.rm_columns(['used', 'use%', 'fs_mount', 'partuuid'])
        net_df = self.fi_info.df
        net_df.loc[:, 'speed'] = 0
        net_df.rm_columns(['version', 'type', 'protocol'])
        net_df.drop_duplicates(inplace=True)
        self.all_net = net_df

    def save(self, path):
        """
        Save the resource graph YAML file

        :param path: the path to save the file
        :return: None
        """
        graph = {
            'hosts': self.hosts,
            'fs': self.all_fs.rows,
            'net': self.all_net.rows,
            'fs_settings': self.fs_settings,
            'net_settings': self.net_settings
        }
        YamlFile(path).save(graph)

    def load(self, path):
        """
        Load resource graph from storage.

        :param path: The path to the resource graph YAML file
        :return: self
        """
        graph = YamlFile(path).load()
        self.hosts = graph['hosts']
        self.all_fs = sdf.SmallDf(graph['fs'], columns=self.fs_columns)
        self.all_net = sdf.SmallDf(graph['net'], columns=self.net_columns)
        self.fs = None
        self.net = None
        self.fs_settings = graph['fs_settings']
        self.net_settings = graph['net_settings']
        self.apply()
        return self

    def set_hosts(self, hosts):
        """
        Set the set of hosts this resource graph covers

        :param hosts: Hostfile()
        :return: None
        """
        self.hosts = hosts.hosts_ip

    def add_storage(self, hosts, **kwargs):
        """
        Register a storage device record

        :param hosts: Hostfile() indicating set of hosts to make record for
        :param kwargs: storage record
        :return: None
        """
        for host in hosts.hosts:
            record = kwargs.copy()
            record['host'] = host
            self.fs_settings['register'].append(record)

    def add_net(self, hosts, **kwargs):
        """
        Register a network record

        :param hosts: Hostfile() indicating set of hosts to make record for
        :param kwargs: net record
        :return: None
        """
        for host, ip in zip(hosts.hosts, hosts.hosts_ip):
            record = kwargs.copy()
            record['fabric'] = ip
            record['host'] = host
            self.net_settings['register'].append(record)

    def filter_fs(self, mount_re,
                  mount_suffix=None, tran=None):
        """
        Track all filesystems + devices matching the mount regex.

        :param mount_re: The regex to match a set of mountpoints
        :param mount_suffix: After the mount_re is matched, append this path
        to the mountpoint to indicate where users can access data. A typical
        value for this is /${USER}, indicating the mountpoint has a subdirectory
        per-user where they can access data.
        :param shared: Whether this mount point is shared
        :param tran: The transport of this device
        :return: self
        """
        self.fs_settings['filter_mounts']['mount_re'] = {
            'mount_re': mount_re,
            'mount_suffix': mount_suffix,
            'tran': tran
        }
        return self

    def filter_ip(self, ip_re, speed=None):
        """
        Track all IPs matching the regex. The IPs with this regex all have
        a certain speed.

        :param ip_re: The regex to match
        :param speed: The speed of the fabric
        :return: self
        """
        self.net_settings['track_ips'][ip_re] = {
            'ip_re': ip_re,
            'speed': SizeConv.to_int(speed) if speed is not None else speed
        }
        return self

    def filter_hosts(self, hosts, speed=None):
        """
        Track all ips matching the hostnames.

        :param hosts: Hostfile() of the hosts to filter for
        :param speed: Speed of the interconnect (e.g., 1gbps)
        :return: self
        """
        for host in hosts.hosts_ip:
            self.filter_ip(host, speed)
        return self

    def filter_net(self, ip_re=None, hosts=None, speed=None):
        if ip_re is not None:
            self.filter_ip(ip_re, speed=speed)
        if hosts is not None:
            self.filter_hosts(hosts, speed=speed)

    def apply(self):
        """
        Apply fs and net settings to the resource graph

        :return: self
        """
        self._apply_fs_settings()
        self._apply_net_settings()
        # self.fs.size = self.fs.size.fillna(0)
        # self.fs.avail = self.fs.avail.fillna(0)
        # self.fs.fs_size = self.fs.fs_size.fillna(0)
        return self

    def _apply_fs_settings(self):
        num_settings = len(self.fs_settings['register']) + \
                       len(self.fs_settings['filter_mounts'])
        if num_settings == 0:
            self.fs = self.all_fs
            self._derive_storage_cols()
            return
        # Get the set of all storage (df)
        df = sdf.SmallDf(self.fs_settings['register'],
                          columns=self.fs_columns)
        df = sdf.concat([self.all_fs, df])
        self.fs = df
        self._derive_storage_cols()

        # Filter the df
        filters = []
        for fs_set in self.fs_settings['filter_mounts'].values():
            mount_re = str(fs_set['mount_re'])
            mount_suffix = fs_set['mount_suffix']
            tran = fs_set['tran']
            with_mount = df[lambda r: re.match(mount_re, str(r['mount']))]
            if mount_suffix is not None:
                with_mount.loc[:, 'mount'] += f'/{mount_suffix}'
            if tran is not None:
                with_mount.loc[:, 'tran'] = tran
            filters.append(with_mount)

        # Create the final filtered df
        self.fs = sdf.concat(filters)

    def _derive_storage_cols(self):
        df = self.fs
        if df is None or len(df) == 0:
            return
        df.loc[lambda r: (r['tran'] == 'sata') and (r['rota'] == True),
               'dev_type'] = str(StorageDeviceType.HDD)
        df.loc[lambda r: (r['tran'] == 'sata') and (r['rota'] == False),
               'dev_type'] = str(StorageDeviceType.SSD)
        df.loc[lambda r: (r['tran'] == 'nvme'),
               'dev_type'] = str(StorageDeviceType.NVME)
        df.loc['mount'].fillna('', inplace=True)
        df.loc['shared'].fillna(True, inplace=True)
        df.loc['tran'].fillna('', inplace=True)

    def _apply_net_settings(self):
        num_settings = len(self.net_settings['register']) + \
                       len(self.net_settings['track_ips'])
        if num_settings == 0:
            self.net = self.all_net
            return
        self.net = sdf.SmallDf(columns=self.all_net.columns)
        df = self.all_net
        for net_set in self.net_settings['track_ips'].values():
            ip_re = net_set['ip_re']
            speed = net_set['speed']
            with_ip = df[df['fabric'].str.contains(ip_re)]
            with_ip.loc[:, 'speed'] = speed
            self.net = sdf.concat([self.net, with_ip])
        admin_df = sdf.SmallDf(self.net_settings['register'],
                                columns=self.net_columns)
        self.net = sdf.concat([self.net, admin_df])
        self._derive_net_cols()

    def _derive_net_cols(self):
        self.net['domain'].fillna('', inplace=True)

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
                     min_avail=None):
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
        :return: Dataframe
        """
        df = self.fs
        # Remove pfs
        df = df[lambda r: r['shared'] == False]
        # Filter devices by whether or not a mount is needed
        if is_mounted:
            df = df[lambda r: r['mount'] != '']
        # Find devices of a particular type
        if dev_types is not None:
            matching_devs = sdf.SmallDf(columns=df.columns)
            if isinstance(dev_types, str):
                dev_types = [dev_types]
            matching_devs = [df[lambda r: r['dev_type'] == dev_type]
                             for dev_type in dev_types]
            matching_devs = sdf.concat(matching_devs)
            df = matching_devs
        # Get the set of mounts common between all hosts
        if common:
            df = df.groupby(['mount']).filter_groups(
                lambda x: len(x) == len(self.hosts)).reset_index()
            if condense:
                df = df.groupby(['mount']).first().reset_index()
        # Remove storage with too little capacity
        if min_cap is not None:
            df = df[lambda r: r['size'] >= min_cap]
        # Remove storage with too little available space
        if min_avail is not None:
            df = df[lambda r: r['avail'] >= min_avail]
        # Take a certain number of each device per-host
        if count_per_dev is not None:
            df = df.groupby(['tran', 'rota', 'host']).\
                head(count_per_dev).reset_index()
        # Take a certain number of matched devices per-host
        if count_per_node is not None:
            df = df.groupby('host').head(count_per_node).reset_index()
        if common and condense:
            df = df.rm_columns('host')
        return df

    @staticmethod
    def _subnet_matches_hosts(subnet, ip_addrs):
        try:
            network = ipaddress.ip_network(subnet, strict=False)
        except:
            return False
        for ip in ip_addrs:
            if ip in network:
                return True
        return False

    def find_net_info(self, hosts,
                      providers=None):
        """
        Find the set of networks common between each host.

        :param hosts: A Hostfile() data structure containing the set of
        all hosts to find network information for
        :param providers: The network protocols to search for.
        :return: Dataframe
        """
        df = self.net
        # Get the set of fabrics corresponding to these hosts
        ips = [ipaddress.ip_address(ip) for ip in hosts.hosts_ip]
        df = df[lambda r: self._subnet_matches_hosts(r['fabric'], ips)]
        # Filter out protocols which are not common between these hosts
        df = df.groupby(['provider', 'domain']).filter_groups(
           lambda x: len(x) >= len(hosts)).reset_index()
        # Choose only a subset of providers
        if providers is not None:
            if not isinstance(providers, (list, set)):
                providers = [providers]
            providers = set(providers)
            df = df[lambda r: r['provider'] in providers]
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
