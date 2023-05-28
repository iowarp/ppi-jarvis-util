"""
This module provides methods for querying the information of the host
system. This can be used to make scripts more portable.
"""

import re
import platform
from jarvis_util.shell.exec import Exec
from jarvis_util.util.size_conv import SizeConv
from jarvis_util.serialize.yaml_file import YamlFile
import json
import pandas as pd
import numpy as np
import shlex


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
        host: the host this record corresponds to
    """

    def __init__(self, exec_info):
        cmd = 'lsblk -o NAME,SIZE,MODEL,TRAN,MOUNTPOINT -J -s'
        super().__init__(cmd, exec_info.mod(collect_output=True))
        self.exec_async = exec_info.exec_async
        self.graph = {}
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        for host, stdout in self.stdout.items():
            lsblk_data = json.loads(self.stdout['localhost'])['blockdevices']
            partitions = []
            devs = {}
            for partition in lsblk_data:
                dev = partition['children'][0]
                partitions.append({
                    'parent': f'/dev/{dev["name"]}',
                    'device': f'/dev/{partition["name"]}',
                    'size': SizeConv.to_int(partition['size']),
                    'mount': partition['mountpoint'],
                    'host': host
                })
                devs[dev['name']] = {
                    'parent': f'/dev/{dev["name"]}',
                    'size': SizeConv.to_int(dev['size']),
                    'model': dev['model'],
                    'tran': dev['tran'],
                    'mount': dev['mountpoint'],
                    'host': host
                }
            devs = list(devs.values())
        part_df = pd.DataFrame(partitions)
        dev_df = pd.DataFrame(devs)
        total_df = pd.merge(part_df,
                            dev_df[['parent', 'model', 'tran', 'host']],
                            on=['parent', 'host'])
        dev_df = dev_df.rename(columns={'parent': 'device'})
        total_df = pd.concat([total_df, dev_df])
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
        self.graph = {}
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        for host, stdout in self.stdout.items():
            devices = stdout.splitlines()
            dev_list = []
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
        df = pd.DataFrame(dev_list)
        df = df.rename(columns={'type': 'fs_type'})
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
        self.graph = {}
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        for host, stdout in self.stdout.items():
            lines = stdout.strip().splitlines()
            columns = ['device', 'fs_size', 'used',
                       'avail', 'use%', 'fs_mount', 'host']
            rows = [line.split() for line in lines[1:]]
            rows.append(host)
            df = pd.DataFrame(rows, columns=columns)
            df.loc[:, 'fs_size'] = df['fs_size'].apply(
                lambda x : SizeConv.to_int(x))
            df.loc[:, 'used'] = df['used'].apply(
                lambda x: SizeConv.to_int(x))
            df.loc[:, 'avail'] = df['avail'].apply(
                lambda x : SizeConv.to_int(x))
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
        for host, stdout in self.stdout.items():
            lines = stdout.strip().splitlines()
            providers = []
            for line in lines:
                if 'provider' in line:
                    providers.append({
                        'provider': line.split(':')[1],
                        'host': host
                    })
                else:
                    splits = line.split(':')
                    key = splits[0].strip()
                    val = splits[1].strip()
                    if 'fabric' in key:
                        val = val.split('/')[0]
                    providers[-1][key] = val
        self.df = pd.DataFrame(providers)


class ResourceGraph:
    """
    Stores helpful information about storage and networking info for machines.

    Two tables are stored to make decisions on application deployment.
    dev:
        parent: the parent device of the partition (e.g., /dev/sda or NaN)
        device: the name of the partition (e.g., /dev/sda1)
        size: total size of the partition (bytes)
        mount: where the partition is mounted (if anywhere)
        model: the exact model of the device
        tran: the transport of the device (e.g., /dev/nvme)
        fs_type: the type of filesystem (e.g., ext4)
        uuid: filesystem-levle uuid from the FS metadata
        partuuid: the partition-lable UUID for the partition
        fs_size: total size of the filesystem
        used: total nubmer of bytes used
        avail: total number of bytes remaining
        use%: the percent of capacity used
        fs_mount: where the filesystem is mounted
        shared: whether the this is a shared service or not
        host: the host this record corresponds to
    net:
        provider: network protocol (e.g., sockets, tcp, ib)
        fabric: IP address
        domain: network domain
        version: network version
        type: packet type (e.g., DGRAM)
        protocol: protocol constant
        host: the host this network corresponds to

    TODO: Need to verify on more than ubuntu20.04
    TODO: Can we make this work for windows?
    TODO: Can we make this work even when hosts have different OSes?
    """

    def __init__(self):
        self.lsblk = None
        self.blkid = None
        self.list_fs = None
        self.fi_info = None
        self.all_fs = {}
        self.all_net = {}
        self.all_fs_settings = {
            'track_mounts': {}
        }
        self.net_settings = {
            'track_ips': {}
        }
        self.hosts = None

    def build(self, exec_info):
        """
        Build a resource graph.
            dev:


        :param exec_info: Where to collect resuorce information
        :return: self
        """
        self.lsblk = Lsblk(exec_info)
        self.blkid = Blkid(exec_info)
        self.list_fs = ListFses(exec_info)
        self.fi_info = FiInfo(exec_info)
        self.hosts = exec_info.hostfile.hosts
        self.all_fs = pd.merge(self.lsblk.df,
                               self.blkid.df,
                               on=['device', 'host'],
                               how='outer')
        self.all_fs['shared'] = False
        self.all_fs = pd.merge(self.all_fs,
                               self.list_fs.df,
                               on=['device', 'host'],
                               how='outer')
        self.all_fs['shared'].fillna(True, inplace=True)
        net_df = self.fi_info.df
        net_df['speed'] = np.nan
        self.all_net = net_df
        return self

    def save(self, path):
        """
        Save the resource graph YAML file

        :param path: the path to save the file
        :return: None
        """
        graph = {
            'fs': self.all_fs.to_dict('records'),
            'net': self.all_net.to_dict('records'),
            'fs_settings': self.all_fs_settings,
            'net_settings': self.all_net_settings
        }
        YamlFile(path).save(graph)

    def load(self, path):
        """
        Load resource graph from storage.

        :param path: The path to the resource graph YAML file
        :return: self
        """
        graph = YamlFile(path).load()
        self.all_fs = pd.DataFrame(graph['fs'])
        self.all_net = pd.DataFrame(graph['net'])
        self.fs = None
        self.net = None
        self.fs_settings = graph['fs_settings']
        self.net_settings = graph['net_settings']
        return self

    def filter_fs(self, mount_re,
                  mount_suffix=None, tran=None):
        """
        Track all storage filesystems + devices matching the mount regex

        :param mount_re: The regex to match a set of mountpoints
        :param mount_suffix: After the mount_re is matched, append this path
        to the mountpoint to indicate where users can access data. A typical
        value for this is /${USER}, indicating the mountpoint has a subdirectory
        per-user where they can access data.
        :param shared: Whether this mount point is shared
        :param tran: The transport of this device
        :return: None
        """
        self.all_fs_settings['track_mounts']['mount_re'] = {
            'mount_re': mount_re,
            'mount_suffix': mount_suffix,
            'shared': shared,
            'tran': tran
        }
        self._apply_fs_settings()

    def filter_ip(self, ip_re, speed=None):
        """
        Track all IPs matching the regex. The IPs with this regex all have
        a certain speed.

        :param ip_re: The regex to match
        :param speed: The speed of the fabric
        :return:
        """
        self.net_settings['track_ips'][ip_re] = {
            'ip_re': ip_re,
            'speed': SizeConv.to_int(speed)
        }
        self._apply_net_settings()

    def track_hosts(self, host_text, speed=None):
        """
        Track all ips matching the hostnames.

        :param host_text: Hostfile text (e.g., ares-comp-[01-32]-40g)
        :param speed: Speed of the interconnect (e.g., 1gbps)
        :return: None
        """
        hosts = Hostfile(text=host_text).hosts_ip
        for host in hosts:
            self.track_ip(host, speed)

    def _apply_fs_settings(self):
        if len(self.fs_settings) == 0:
            self.fs = self.all_fs
            return
        df = self.all_fs
        self.fs = pd.DataFrame(columns=self.all_net.columns)
        for fs_set in self.fs_settings['track_mounts'].values():
            mount_re = fs_set['mount_re']
            mount_suffix = fs_set['mount_suffix']
            shared = fs_set['shared']
            tran = fs_set['tran']
            with_mount = df[df['mount'].str.contains(mount_re)]
            if mount_suffix is not None:
                with_mount['mount'] += mount_suffix
            if shared is not None:
                with_mount['shared'] = shared
            if tran is not None:
                with_mount['tran'] = tran
            self.fs = pd.concat([self.fs, with_mount])

    def _apply_net_settings(self):
        if len(self.net_settings) == 0:
            self.net = self.all_net
            return
        self.net = pd.DataFrame(columns=self.all_net.columns)
        df = self.all_net
        for net_set in self.net_settings['track_ips'].values():
            ip_re = net_set['ip_re']
            speed = net_set['speed']
            with_ip = df[df['fabric'].str.contains(ip_re)]
            with_ip['speed'] = speed
            self.net = pd.concat([self.net, with_ip])

    def find_storage(self, tran=None,
                     is_mounted=False,
                     common=False,
                     count_per_node=None,):
        """
        Find a set of storage devices.

        :param tran: A specific transport to search for (e.g., NVME)
        :param is_mounted: Search only for mounted devices
        :param common: Remove mount points that are not common across hosts
        :param count_per_node: Choose only a subset of devices matching query
        :return: None
        """
        df = self.fs
        result = pd.DataFrame(columns=df.columns)
        # Filter devices by transport
        if tran is not None:
            res = [df[df.tran == transport]
                   for transport in tran]
            result = pd.concat(res + result)
        else:
            result = df
        # Filter devices by whether or not a mount is needed
        if is_mounted:
            df = df[df.mount.notna()]
        # Get the set of mounts common between all hosts
        if common:
            df = df.groupby(['mount']).filter(
                lambda x: len(x) == len(self.hosts)).reset_index()
        # Take a certain number of matched devices per-host
        if count_per_node is not None:
            df = df.groupby('host').head(count_per_node).reset_index()
        return df.to_dict('record')

    def find_net_info(self, hosts, provider=None):
        """
        Find the set of networks common between each host.

        :param hosts:
        :param provider: The provider
        :return:
        """
        pass
