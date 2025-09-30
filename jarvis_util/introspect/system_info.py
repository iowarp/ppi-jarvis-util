"""
This module provides methods for querying the information of the host
system. This can be used to make scripts more portable.
"""

import sys
import socket
import re
import platform
from jarvis_util.util.logging import ColorPrinter, Color
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo
from jarvis_util.shell.mpi_exec import MpiExecInfo
from jarvis_util.util.size_conv import SizeConv
from jarvis_util.serialize.yaml_file import YamlFile
from jarvis_util.shell.process import Kill
from jarvis_util.util.hostfile import Hostfile
import jarvis_util.util.small_df as sdf
import threading
import json
import yaml
import shlex
import ipaddress
import copy
import time
import os
from pathlib import Path
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
    columns = [
        'parent', 'device', 'size', 'mount', 'model', 'tran',
        'rota', 'dev_type', 'host'
    ]

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
            try:
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
            except json.JSONDecodeError:
                pass
        self.df = sdf.SmallDf(rows=total, columns=self.columns)

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
    columns = [
        'parent', 'device', 'size', 'mount', 'model', 'tran',
        'rota', 'dev_type', 'host'
    ]
    def __init__(self, exec_info):
        cmd = 'pylsblk'
        super().__init__(cmd, exec_info.mod(collect_output=True, hide_output=False))
        self.exec_async = exec_info.exec_async
        self.df = None
        if not self.exec_async:
            self.wait()

    def wait(self):
        super().wait()
        total = []
        for host, stdout in self.stdout.items():
            lsblk_data = yaml.load(stdout, Loader=yaml.FullLoader)
            if not lsblk_data:
                print(f'Warning: no storage devices found on host {host}')
                print(f'STDOUT: \n{stdout}')
                continue
            for dev in lsblk_data:
                if dev['tran'] == 'pcie':
                    dev['tran'] = 'nvme'
                dev['dev_type'] = self.GetDevType(dev)
                dev['host'] = host
                total.append(dev)
        self.df = sdf.SmallDf(rows=total, columns=self.columns)

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


class ChiNetPing(Exec):
    """
    Determine whether a network functions across a set of hosts
    """
    def __init__(self, provider, domain, port, mode, 
                 local_only, exec_info, hostfile=None, 
                 timeout=None):
        if hostfile is None:
            hostfile = exec_info.hostfile
        hostfile = hostfile.path if hostfile.path else '\"\"'
        self.cmd = [
            'chi_net_ping',
            hostfile,
            f'\'{provider}\'',
            f'\'{domain}\'',
            str(port),
            mode,
            local_only
        ]
        self.cmd = ' '.join(self.cmd)
        if mode == 'server':
            super().__init__(self.cmd, exec_info.mod(
                exec_async=True, hide_output=False,
                timeout=timeout))
        else:
            super().__init__(self.cmd, LocalExecInfo(env=exec_info.env, 
            hide_output=False, timeout=timeout))


class ChiNetPingTest:
    """
    Determine whether a network functions across a set of hosts
    """
    def __init__(self, provider, domain, port, local_only,
                  exec_info, net_sleep=10, hostfile=None, timeout=5): 
        netping_timeout = net_sleep + timeout + 1
        self.server = ChiNetPing(provider, domain, port, "server", local_only,
                                 exec_info.mod(exec_async=True),
                                   hostfile=hostfile, timeout=netping_timeout)
        print(f'Server timeout: {net_sleep}')
        time.sleep(net_sleep)
        print(f'Client timeout: {timeout}') 
        self.client = ChiNetPing(provider, domain, port, "client", local_only, 
                                 exec_info.mod(exec_async=True),
                                 hostfile=hostfile,
                                 timeout=timeout)  
        time.sleep(timeout)
        print(f'Timeout finished')
        self.client.wait()
        self.exit_code = self.client.exit_code
        print(f'Client finished: {self.client.exit_code}')


class NetTest:
    """
    Determine whether a set of networks function across a set of hosts.
    """
    def __init__(self, fi_info_df, port, exec_info, 
                 exclusions=None, base_port=6040, net_sleep=10, local_only=False, 
                 server_start_only=False, timeout=5):
        self.local_only = local_only
        self.server_start_only = server_start_only
        self.working = [] 
        df = fi_info_df[['provider', 'domain', 'fabric']].drop_duplicates()
        if exclusions:
            exclusions = exclusions[['provider', 'domain', 'fabric']].drop_duplicates()
            df = df[lambda r: r not in exclusions]
        self.net_count = len(df)
        ColorPrinter.print(f'About to test {self.net_count} networks', Color.YELLOW)
        port = base_port
        threads = []
        self.results = [None] * len(df)
        self.timeout = timeout
        for idx, net in enumerate(df.rows): 
            self._async_test(idx, net, port, exec_info, net_sleep)
            port += 2

        # Wait for all threads to complete    
        for idx in range(len(df)):  
            result = self.results[idx]
            if result is not None:
                self.working.append(result)
            
        self.df = sdf.SmallDf(self.working)
        Kill('chi_net_ping', exec_info)

    def _async_test(self, idx, net, port, exec_info, net_sleep):
        if self.server_start_only:
            self.touch_test(idx, net, port, exec_info)
        else:
            self.roundtrip_test(idx, net, port, exec_info, net_sleep)

    def touch_test(self, idx, net, port, exec_info):
        provider = net['provider']
        domain = net['domain']
        fabric = net['fabric']
        # Create the output hostfile
        ColorPrinter.print(f'Testing {idx + 1}/{self.net_count} {provider}://{domain}/[{fabric}]:{port}', Color.CYAN)
        out_hostfile = os.path.join(Path.home(), '.jarvis', 'hostfiles', f'hosts.{idx}')
        os.makedirs(os.path.dirname(out_hostfile), exist_ok=True)
        compile = CompileHostfile(LocalExecInfo().hostfile, provider, domain, 
                                  fabric, out_hostfile, env=exec_info.env)
        # Run the ping test
        ping = ChiNetPing(provider, domain, port, "touchserver", "local",
                           exec_info, hostfile=compile.hostfile)
        if ping.exit_code != 0:
            ColorPrinter.print(f'EXCLUDING the network {provider}://{domain}/[{fabric}]:{port}: {ping.exit_code}', Color.YELLOW)
            ColorPrinter.print(f'EXCLUDING the network {provider}://{domain}/[{fabric}]:{port}: {ping.exit_code}', Color.YELLOW, file=sys.stderr)
        else:
            ColorPrinter.print(f'INCLUDING the network {provider}://{domain}/[{fabric}]:{port}', Color.GREEN)
            self.results[idx] = net

    def roundtrip_test(self, idx, net, port, exec_info, net_sleep):
        provider = net['provider']
        domain = net['domain']
        fabric = net['fabric']
        ColorPrinter.print(f'Testing {idx + 1}/{self.net_count} {provider}://{domain}/[{fabric}]:{port}', Color.CYAN)
        # Create the output hostfile
        out_hostfile = os.path.join(Path.home(), '.jarvis', 'hostfiles', f'hosts.{idx}')
        os.makedirs(os.path.dirname(out_hostfile), exist_ok=True)
        compile = CompileHostfile(exec_info.hostfile, provider, domain, 
                                  fabric, out_hostfile, env=exec_info.env)
        # Test if the network works locally
        ping = ChiNetPingTest(provider, domain, port, "local", 
                              exec_info, hostfile=compile.hostfile, timeout=5, net_sleep=5)
        net['shared'] = False
        shared = 'local'
        if ping.exit_code != 0:
            ColorPrinter.print(f'EXCLUDING the network {provider}://{domain}/[{fabric}]:{port} (hostfile={out_hostfile}): {ping.exit_code}', Color.YELLOW)
            ColorPrinter.print(f'EXCLUDING the network {provider}://{domain}/[{fabric}]:{port} (hostfile={out_hostfile}): {ping.exit_code}', Color.YELLOW, file=sys.stderr)
            return
        self.results[idx] = net
        port += 1
        if not self.local_only and domain != 'lo':
            # Test if the network works across hosts
            ping = ChiNetPingTest(provider, domain, port, "all", 
                                  exec_info, net_sleep, hostfile=compile.hostfile, timeout=self.timeout)
            if ping.exit_code == 0:
                net['shared'] = True
                shared = 'shared'
        ColorPrinter.print(f'INCLUDING the {shared} network {provider}://{domain}/[{fabric}]:{port}', Color.GREEN)
        ColorPrinter.print(f'INCLUDING the {shared} network {provider}://{domain}/[{fabric}]:{port}', Color.GREEN, file=sys.stderr)


class CompileHostfile(Exec):
    def __init__(self, cur_hosts, provider, domain, fabric, out_hostfile, env=None):
        cmd = [
            'chi_net_find',
            f'"{provider}"',
            f'"{domain}"',
            f'"{fabric}"',
            out_hostfile
        ]
        cmd = ' '.join(cmd)
        super().__init__(cmd, MpiExecInfo(env=env, hosts=cur_hosts, ppn=1, 
                                          nprocs=len(cur_hosts), hide_output=True))
        self.hostfile = Hostfile(path=out_hostfile)


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
        needs_root: whether the user needs sudo to access the device
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
        self.fs_columns = [
            'parent', 'device', 'mount', 'model', 'dev_type',
            'fs_type', 'uuid',
            'avail', 'shared', 'needs_root'
        ]
        self.net_columns = [
            'provider', 'fabric', 'domain',
            'speed', 'shared'
        ]
        self.create()
        self.path = None

    """
    Build the resource graph
    """

    def create(self):
        self.fs = sdf.SmallDf(columns=self.fs_columns)
        self.net = sdf.SmallDf(columns=self.net_columns)

    def build(self, exec_info, introspect=True, net_sleep=10):
        """
        Build a resource graph.

        :param exec_info: Where to collect resource information
        :param introspect: Whether to pylsblk system info, or rely solely
        on admin-defined settings
        :return: self
        """
        self.create()
        if introspect:
            self.introspect_fs(exec_info)
            self.introspect_net(exec_info, prune_nets=True, net_sleep=net_sleep)
        self.apply()
        return self
    
    def modify(self, exec_info, net_sleep):
        """
        Edit a resource graph with new information
        
        """
        self.introspect_fs(exec_info)
        self.introspect_net(exec_info, prune_nets=True, net_sleep=net_sleep)
        self.apply()

    """
    Introspect filesystems
    """

    def introspect_fs(self, exec_info, sudo=False):
        lsblk = PyLsblk(exec_info.mod(hide_output=True))  
        blkid = Blkid(exec_info.mod(hide_output=True))
        list_fs = ListFses(exec_info.mod(hide_output=True))
        fs = sdf.merge([lsblk.df, blkid.df],
                          on=['device', 'host'],
                          how='outer') 
        fs[:, 'shared'] = False
        fs = sdf.merge([fs, list_fs.df],
                            on=['device', 'host'],
                            how='outer')
        fs['mount'] = fs['fs_mount'] 
        fs = self._find_common_mounts(fs, exec_info)
        fs = self._label_user_mounts(fs)
        fs = fs.drop_columns([
            'used', 'use%', 'fs_mount', 'partuuid', 'fs_size',
            'partlabel', 'label', 'host'])
        # Filter out all devices that begin with /run
        exclusions = ['/run', '/sys', '/proc', '/dev/shm', '/boot']
        fs = fs.loc(lambda r: r['mount'] and not r['needs_root']
                     and not any(r['mount'].startswith(ex) for ex in exclusions)
                     and not r['device'] == 'tmpfs')
        self.fs = fs
        return self.fs
    
    def _find_common_mounts(self, fs, exec_info):
        """
        Finds mount point points common across all hosts
        """ 
        io_groups = fs.groupby(['mount', 'device'])
        common = []
        for name, group in io_groups.groups.items():
            if len(group) != len(exec_info.hostfile.hosts):
                continue
            common.append(group.rows[0])
        return sdf.SmallDf(common)

    def _label_user_mounts(self, fs):
        """
        Try to find folders/directories the current user
        can access without root priveleges in each mount
        """
        for dev in fs.rows:
            dev['needs_root'] = True
            if dev['mount'] is None:
                continue
            if not dev['mount'].startswith('/'):
                continue
            dev['mount'] = self._try_user_access_paths(dev, fs)
        return sdf.SmallDf(fs.rows)

    def _try_user_access_paths(self, dev, fs):
        username = os.getenv('USER') or os.getenv('USERNAME')
        paths = [
            dev['mount'], 
            os.path.join(dev["mount"], username),
            os.path.join(dev["mount"], 'users', username),
            os.path.join(dev["mount"], 'home', username),
        ]
        for path in paths:
            if self._try_user_access(fs, path, path == dev['mount']):
                dev['needs_root'] = False
                return path
        dev['needs_root'] = True
        return dev['mount']

    def _try_user_access(self, fs, mount, known_mount=False):
        try:
            if mount.startswith('/boot'):
                print(mount)
            if not known_mount and self._check_if_mounted(fs, mount):
                return False
            test_file = os.path.join(mount, '.jarvis_access')
            with open(test_file, 'w') as f:
                pass
            with open(test_file, 'r') as f:
                pass
            os.remove(test_file)
            return True
        except (PermissionError, OSError):
            return False

    def _check_if_mounted(self, fs, mount):
        return len(fs[lambda r: r['mount'] == mount]) > 0

    """
    Introspect networks
    """

    def introspect_net(self, exec_info, prune_nets=False, prune_port=4192, net_sleep=10):
        fi_info = FiInfo(exec_info.mod(hide_output=True))
        if prune_nets:
            fi_info = NetTest(fi_info.df, prune_port, exec_info.mod(hide_output=True), 
            exclusions=self.net, net_sleep=net_sleep, server_start_only=True)
        net_df = fi_info.df
        net_df[:, 'speed'] = 0
        net_df.drop_columns(['version', 'type', 'protocol'])
        net_df.drop_duplicates()
        if self.net:
            self.net = sdf.concat([self.net, net_df])
        else:
            self.net = net_df

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

    def save(self, path):
        """
        Save the resource graph YAML file

        :param path: the path to save the file
        :return: None
        """
        graph = {
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
        noavail = df[lambda r: r['avail'] == 0 or r['avail'] is None, :]
        noavail['avail'] = noavail['size']
        df['avail'].apply(lambda r, c: SizeConv.to_int(r[c]))
        df['size'] = df['avail']

    def _derive_net_cols(self):
        self.net['domain'].fillna('')

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

    def find_user_storage(self):
        """
        Find the set of user-accessible storage services

        :return: Dataframe
        """
        df = self.fs
        return df[lambda r: r['needs_root'] == False]

    def find_storage(self,
                     dev_types=None,
                     is_mounted=True,
                     needs_root = None,
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
        column and will only contain one entry per mount point.
        :param needs_root: Search for devices that do or don't need root access.
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
        # Filter devices by whether or not root is needed
        if needs_root is not None:
            df = df[lambda r: r['needs_root'] == needs_root]
        # Find devices of a particular type
        if dev_types is not None:
            if not isinstance(dev_types, (list, tuple, set)):
                dev_types = [dev_types]
            df = df[lambda r: str(r['dev_type']) in dev_types]
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
        #     df = df.drop_columns('host')
        if shared is not None:
            df = df[lambda r: r['shared'] == shared]
        return df

    def find_net_info(self,
                      hosts=None,
                      strip_ips=False,
                      providers=None,
                      local=True,
                      shared=True,
                      df=None,
                      prune_port=6040,
                      env=None):
        """
        Find the set of networks common between each host.

        :param hosts: A Hostfile() data structure containing the set of
        all hosts to find network information for
        :param strip_ips: remove IPs that are not compatible with the hostfile
        :param providers: The network protocols to search for.
        :param df: The df to use for this query
        :param local: Filter local networks
        :param shared: Filter shared networks
        :param prune_port: The port to use for network testing
        :param net_sleep: The time to sleep between network tests
        :param env: Environment for the net ping tests
        :return: Dataframe
        """
        if df is None:
            df = self.net
        # Choose only a subset of providers
        if providers is not None:
            if not isinstance(providers, (list, set)):
                providers = [providers]
            providers = set(providers)
            df = df[lambda r: r['provider'] in providers]
        # Remove shared networks
        if not shared:
            df = df[lambda r: r['shared'] != True]
        # Remove local networks
        if not local:
            df = df[lambda r: r['shared'] != False]
        # Test validitiy of networks for current hostfile
        if hosts is not None and strip_ips:
            # Perform a local net-test to see if we can start a server 
            fi_info = NetTest(df, prune_port, 
                    LocalExecInfo(hostfile=hosts, env=env, hide_output=True),
                    net_sleep=1, local_only=True, server_start_only=True)
            df = fi_info.df
        return df

    def print_df(self, df):
        if 'device' in df.columns:
            print(df.sort_values('mount').to_string())
        else:
            print(df.sort_values('provider').to_string())


# pylint: enable=C0121
