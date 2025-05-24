"""
This module provides various wrappers for methods which manage processes
in the cluster. Examples include killing processes, determining whether
or not a process exists, etc.
"""

from .exec import Exec


class Kill(Exec):
    """
    Kill all processes which match the name regex.
    """

    def __init__(self, cmd, exec_info, partial=True):
        """
        Kill all processes which match the name regex.

        :param cmd: A regex for the command to kill
        :param exec_info: Info needed to execute the command
        """
        partial_cmd = "-f" if partial else ""
        super().__init__(f"pkill -9 {partial_cmd} {cmd}", exec_info)


class SetAffinity(Exec):
    """
    Set CPU affinity for a process.
    """

    def __init__(self, pid, cpu_list, exec_info):
        """
        Set CPU affinity for a specific process.

        :param pid: Process ID
        :param cpu_list: List of CPU cores to set affinity to
        :param exec_info: Info needed to execute the command
        """
        cpu_string = ",".join(map(str, cpu_list))
        super().__init__(f"taskset -pc {cpu_string} {pid}", exec_info)
