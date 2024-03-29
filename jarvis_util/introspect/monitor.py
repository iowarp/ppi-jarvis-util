from jarvis_util.shell.exec import Exec
from jarvis_util.serialize.yaml_file import YamlFile
import os
import yaml

class Callgrind(Exec):
    def __init__(self, cmd, exec_info=None):
        super().__init__(f'valgrind --tool=callgrind {cmd}')


class Monitor(Exec):
    def __init__(self, frequency_sec, monitor_dir, exec_info=None):
        super().__init__(
            f'pymonitor {frequency_sec} {monitor_dir}', exec_info)


class MonitorParser:
    def __init__(self, monitor_dir):
        self.monitor_dir = monitor_dir
        self.disk = {}
        self.net = {}
        self.mem = {}
        self.cpu = {}

    def parse(self):
        paths = os.listdir(self.monitor_dir)
        for hostname in paths:
            path = os.path.join(self.monitor_dir, hostname)
            with open(path, 'r') as fp:
                lines = fp.readlines()
                for line in lines:
                    try:
                        yaml_dict = yaml.load(line, Loader=yaml.FullLoader)
                    except yaml.YAMLError:
                        continue
                    if yaml_dict['type'] == 'DSK':
                        if hostname not in self.disk:
                            self.disk[hostname] = []
                        self.disk[hostname].append(yaml_dict)
                    elif yaml_dict['type'] == 'NET':
                        if hostname not in self.net:
                            self.net[hostname] = []
                        self.net[hostname].append(yaml_dict)
                    elif yaml_dict['type'] == 'MEM':
                        if hostname not in self.mem:
                            self.mem[hostname] = []
                        self.mem[hostname].append(yaml_dict)
                    elif yaml_dict['type'] == 'CPU':
                        if hostname not in self.cpu:
                            self.cpu[hostname] = []
                        self.cpu[hostname].append(yaml_dict)

    def avg_memory(self):
        total = 0
        count = 0
        for hostname in self.mem:
            for mem in self.mem[hostname]:
                total += mem['percent']
                count += 1
        return total / count

    def peak_memory(self):
        peak = 0
        for hostname in self.mem:
            for mem in self.mem[hostname]:
                peak = max(peak, mem['percent'])
        return peak

    def avg_cpu(self):
        total = 0
        count = 0
        for hostname in self.cpu:
            for cpu in self.cpu[hostname]:
                total += cpu['percent']
                count += 1
        return total / count
