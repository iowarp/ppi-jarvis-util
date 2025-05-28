"""
This module contains various wrappers over typical filesystem commands seen
in shell scripts. This includes operations such as creating directories,
changing file permissions, etc.
"""
from .exec import Exec


class Mkdir(Exec):
    """
    Create directories + subdirectories.
    """

    def __init__(self, paths, exec_info=None):
        """
        Create directories + subdirectories. Does not fail if the dirs
        already exist.

        :param paths: A list of paths or a single path string.
        :param exec_info: Info needed to execute the mkdir command
        """

        if isinstance(paths, str):
            paths = [paths]
        path = ' '.join(paths)
        super().__init__(f'mkdir -p {path}', exec_info)


class Rm(Exec):
    """
    Remove a file and its subdirectories
    """

    def __init__(self, paths, exec_info=None):
        """
        Execute file or directory remove.

        :param paths: Either a list of paths or a single path string
        :param exec_info: Information needed to execute rm
        """

        if isinstance(paths, str):
            paths = [paths]
        path = ' '.join(paths)
        super().__init__(f'rm -rf {path}', exec_info)


class Chmod(Exec):
    """
    Change the mode of a file
    """

    def __init__(self, path=None, mode=None, modes=None, exec_info=None):
        """
        Change the mode of a file

        :param path: path to file to mode change
        :param mode: the mode to change to
        :param modes: A list of tuples [(Path, Mode)]
        :param exec_info: How to execute commands
        """
        cmds = []
        if path is not None and mode is not None:
            cmds.append(f'chmod {mode} {path}')
        if modes is not None:
            cmds += [f'chmod {mode[1]} {mode[0]}' for mode in modes]
        if len(cmds) == 0:
            raise Exception('Must set either path+mode or modes')
        super().__init__(cmds, exec_info)


class Chown(Exec):
    """
    Change the owner of a file
    """

    def __init__(self, path, user, group, exec_info=None):
        """
        Change the owner of a file

        :param path: path to file to chown
        :param user: user to chown to
        :param group: group to chown to
        :param exec_info: How to execute commands
        """
        super().__init__(f'chown {user}:{group} {path}', exec_info)
        

class Copy(Exec):
    """
    Change the mode of a file
    """

    def __init__(self, target=None, destination=None, exec_info=None):
        """
        Change the mode of a file

        :param target: path to file to copy
        :param destination: destination of the new file
        """
        if isinstance(target, str) and isinstance(destination, str):
            super().__init__(f'cp -r {target} {destination}', exec_info)
        else:
            raise Exception('target and destination must be strings')


class MkfsExt4(Exec):
    """
    Create an EXT4 filesystem on a device or partition
    """

    def __init__(self, device, force=False, exec_info=None, **kwargs):
        """
        Create an EXT4 filesystem on a device or partition.

        :param device: Path to the device or partition (e.g. /dev/sda1)
        :param force: Whether to force creation without prompting (defaults to False)
        :param exec_info: Information needed to execute mkfs
        :param kwargs: Additional EXT4 filesystem options:
            - block_size: Block size in bytes (must be power of 2)
            - label: Label for the filesystem (max 16 bytes)
            - bytes_per_inode: Number of bytes per inode (ratio for inode creation)
            - journal: Enable/disable journaling (True/False)
            - journal_device: External journal device path
            - num_inodes: Number of inodes to create
            - flex_bg_size: Size of a flex block group
            - reserved_blocks_percentage: Percentage of blocks reserved for super-user
            - stripe_width: RAID stripe width in blocks
            - extent: Enable/disable extents feature (True/False)
            - extra_isize: Additional inode space in bytes
            - quota: Enable quota support (user, group, project)
            - cluster_size: Cluster size in bytes
            - metadata_checksum: Enable metadata checksums (True/False)
        """
        cmd = 'mkfs.ext4'
        
        if force:
            cmd += ' -F'

        # Basic options
        if 'block_size' in kwargs:
            cmd += f' -b {kwargs["block_size"]}'
        
        if 'label' in kwargs:
            cmd += f' -L {kwargs["label"]}'

        if 'bytes_per_inode' in kwargs:
            cmd += f' -i {kwargs["bytes_per_inode"]}'

        # Journal options
        if 'journal' in kwargs and not kwargs['journal']:
            cmd += ' -O ^has_journal'
        if 'journal_device' in kwargs:
            cmd += f' -J device={kwargs["journal_device"]}'

        # Inode and block group options
        if 'num_inodes' in kwargs:
            cmd += f' -N {kwargs["num_inodes"]}'
        if 'flex_bg_size' in kwargs:
            cmd += f' -G {kwargs["flex_bg_size"]}'
        if 'reserved_blocks_percentage' in kwargs:
            cmd += f' -m {kwargs["reserved_blocks_percentage"]}'

        # Performance options
        if 'stripe_width' in kwargs:
            cmd += f' -E stride={kwargs["stripe_width"]}'
        if 'cluster_size' in kwargs:
            cmd += f' -C {kwargs["cluster_size"]}'

        # Feature flags
        if 'extent' in kwargs:
            cmd += ' -O extent' if kwargs['extent'] else ' -O ^extent'
        if 'extra_isize' in kwargs:
            cmd += f' -I {kwargs["extra_isize"]}'
        if 'quota' in kwargs:
            cmd += ' -O quota'
        if 'metadata_checksum' in kwargs:
            cmd += ' -O metadata_csum' if kwargs['metadata_checksum'] else ' -O ^metadata_csum'

        cmd += f' {device}'
        super().__init__(cmd, exec_info)


class MkfsXfs(Exec):
    """
    Create an XFS filesystem on a device or partition
    """

    def __init__(self, device, force=False, exec_info=None, **kwargs):
        """
        Create an XFS filesystem on a device or partition.

        :param device: Path to the device or partition (e.g. /dev/sda1)
        :param force: Whether to force creation without prompting (defaults to False)
        :param exec_info: Information needed to execute mkfs
        :param kwargs: Additional XFS filesystem options:
            - block_size: Block size in bytes (must be power of 2)
            - label: Label for the filesystem
            
            Data section options:
            - agcount: Number of allocation groups
            - data_sunit: Stripe unit for data section (in 512B blocks)
            - data_swidth: Stripe width for data section (in 512B blocks)
            
            Inode options:
            - isize: Inode size in bytes
            - sparse: Enable sparse inode allocation (0 or 1)
            
            Log section options:
            - logsize: Size of the log section in bytes
            - log_sunit: Stripe unit for log section (in 512B blocks)
            - log_internal: Whether log is internal (True) or external (False)
            - log_device: Device path for external log
            
            Real-time section options:
            - rt_device: Device path for real-time section
            - rt_extsize: Real-time extent size in bytes
            
            Metadata options:
            - lazy_count: Enable/disable lazy-count feature (0 or 1)
            - bigtime: Enable timestamps beyond 2038 (0 or 1)
            - finobt: Enable free inode btree (0 or 1)
            - sparse: Enable sparse inode allocation (0 or 1)
            - rmapbt: Enable reverse mapping btree (0 or 1)
            - reflink: Enable reflink feature (0 or 1)
            - metadata_crc: Enable metadata CRC feature (0 or 1)
        """
        cmd = 'mkfs.xfs'
        
        if force:
            cmd += ' -f'

        # Basic options
        if 'block_size' in kwargs:
            cmd += f' -b size={kwargs["block_size"]}'
        
        if 'label' in kwargs:
            cmd += f' -L {kwargs["label"]}'

        # Data section options
        data_opts = []
        if 'agcount' in kwargs:
            data_opts.append(f'agcount={kwargs["agcount"]}')
        if 'data_sunit' in kwargs:
            data_opts.append(f'sunit={kwargs["data_sunit"]}')
        if 'data_swidth' in kwargs:
            data_opts.append(f'swidth={kwargs["data_swidth"]}')
        if data_opts:
            cmd += f' -d {",".join(data_opts)}'

        # Inode options
        inode_opts = []
        if 'isize' in kwargs:
            inode_opts.append(f'size={kwargs["isize"]}')
        if 'sparse' in kwargs:
            inode_opts.append(f'sparse={1 if kwargs["sparse"] else 0}')
        if inode_opts:
            cmd += f' -i {",".join(inode_opts)}'

        # Log section options
        log_opts = []
        if 'logsize' in kwargs:
            log_opts.append(f'size={kwargs["logsize"]}')
        if 'log_sunit' in kwargs:
            log_opts.append(f'sunit={kwargs["log_sunit"]}')
        if 'log_internal' in kwargs and not kwargs['log_internal']:
            log_opts.append('internal=0')
        if 'log_device' in kwargs:
            log_opts.append(f'logdev={kwargs["log_device"]}')
        if log_opts:
            cmd += f' -l {",".join(log_opts)}'

        # Real-time section options
        rt_opts = []
        if 'rt_device' in kwargs:
            rt_opts.append(f'rtdev={kwargs["rt_device"]}')
        if 'rt_extsize' in kwargs:
            rt_opts.append(f'extsize={kwargs["rt_extsize"]}')
        if rt_opts:
            cmd += f' -r {",".join(rt_opts)}'

        # Metadata options
        meta_opts = []
        if 'lazy_count' in kwargs:
            meta_opts.append(f'lazy-count={kwargs["lazy_count"]}')
        if 'bigtime' in kwargs:
            meta_opts.append(f'bigtime={kwargs["bigtime"]}')
        if 'finobt' in kwargs:
            meta_opts.append(f'finobt={kwargs["finobt"]}')
        if 'rmapbt' in kwargs:
            meta_opts.append(f'rmapbt={kwargs["rmapbt"]}')
        if 'reflink' in kwargs:
            meta_opts.append(f'reflink={kwargs["reflink"]}')
        if 'metadata_crc' in kwargs:
            meta_opts.append(f'crc={1 if kwargs["metadata_crc"] else 0}')
        if meta_opts:
            cmd += f' -m {",".join(meta_opts)}'

        cmd += f' {device}'
        super().__init__(cmd, exec_info)


class MkfsF2fs(Exec):
    """
    Create a Flash-Friendly File System (F2FS) on a device or partition
    """

    def __init__(self, device, force=False, exec_info=None, **kwargs):
        """
        Create an F2FS filesystem on a device or partition.

        :param device: Path to the device or partition (e.g. /dev/sda1)
        :param force: Whether to force creation without prompting (defaults to False)
        :param exec_info: Information needed to execute mkfs
        :param kwargs: Additional F2FS filesystem options:
            - label: Volume label (up to 512 bytes)
            - segment_count: Number of segments per section
            - sectors_per_blk: Size of block in sectors (default: 4)
            - sections_per_zone: Number of sections per zone (default: 1)
            - trim: Enable/disable TRIM (default: True)
            - coverage: Space utilization in percentage (default: 100)
            - overprovision: Percentage of overprovision area (default: 5)
            - zoned: Configure zoned block device support
        """
        cmd = ['mkfs.f2fs']
        if force:
            cmd.append('-f')
        
        if 'label' in kwargs:
            cmd.extend(['-l', str(kwargs['label'])])
        if 'segment_count' in kwargs:
            cmd.extend(['-c', str(kwargs['segment_count'])])
        if 'sectors_per_blk' in kwargs:
            cmd.extend(['-s', str(kwargs['sectors_per_blk'])])
        if 'sections_per_zone' in kwargs:
            cmd.extend(['-z', str(kwargs['sections_per_zone'])])
        if 'trim' in kwargs and not kwargs['trim']:
            cmd.append('-t')
        if 'coverage' in kwargs:
            cmd.extend(['-u', str(kwargs['coverage'])])
        if 'overprovision' in kwargs:
            cmd.extend(['-r', str(kwargs['overprovision'])])
        if 'zoned' in kwargs and kwargs['zoned']:
            cmd.append('-m')

        cmd.append(device)
        super().__init__(' '.join(cmd), exec_info)


class MkfsBtrfs(Exec):
    """
    Create a BTRFS filesystem on a device or partition
    """

    def __init__(self, devices, force=False, exec_info=None, **kwargs):
        """
        Create a BTRFS filesystem on one or more devices.

        :param devices: Single device path or list of device paths
        :param force: Whether to force creation without prompting (defaults to False)
        :param exec_info: Information needed to execute mkfs
        :param kwargs: Additional BTRFS filesystem options:
            - label: Volume label
            - metadata_profile: Metadata profile (raid0, raid1, raid1c3, raid1c4, raid5, raid6, dup, single)
            - data_profile: Data profile (same options as metadata_profile)
            - mixed: Enable mixed block groups for metadata and data
            - nodesize: Node size in bytes (default: 16384)
            - sectorsize: Sector size in bytes (default: 4096)
            - features: List of features to enable
            - checksum: Checksum algorithm (crc32c, xxhash, sha256, blake2)
        """
        cmd = ['mkfs.btrfs']
        if force:
            cmd.append('-f')
        
        if 'label' in kwargs:
            cmd.extend(['-L', str(kwargs['label'])])
        if 'metadata_profile' in kwargs:
            cmd.extend(['-m', kwargs['metadata_profile']])
        if 'data_profile' in kwargs:
            cmd.extend(['-d', kwargs['data_profile']])
        if kwargs.get('mixed', False):
            cmd.append('--mixed')
        if 'nodesize' in kwargs:
            cmd.extend(['--nodesize', str(kwargs['nodesize'])])
        if 'sectorsize' in kwargs:
            cmd.extend(['--sectorsize', str(kwargs['sectorsize'])])
        if 'features' in kwargs:
            cmd.extend(['--features', ','.join(kwargs['features'])])
        if 'checksum' in kwargs:
            cmd.extend(['--checksum', kwargs['checksum']])

        if isinstance(devices, str):
            devices = [devices]
        cmd.extend(devices)
        super().__init__(' '.join(cmd), exec_info)


class MkfsZfs(Exec):
    """
    Create a ZFS filesystem on a device or pool
    """

    def __init__(self, name, exec_info=None, **kwargs):
        """
        Create a ZFS filesystem in a pool.

        :param name: Name of the filesystem to create (pool/dataset)
        :param exec_info: Information needed to execute zfs
        :param kwargs: Additional ZFS filesystem options:
            - mountpoint: Custom mount point
            - compression: Compression algorithm (on, off, lzjb, gzip, zle, lz4)
            - atime: Update access time on read (on/off)
            - quota: Limit on how much space can be used
            - reservation: Guaranteed space for dataset
            - recordsize: Record size (power of 2 between 512B and 1M)
            - dedup: Enable deduplication (on/off)
            - encryption: Encryption algorithm (aes-128-ccm, aes-192-ccm, aes-256-ccm)
            - keylocation: Location of the encryption key
            - keyformat: Format of the encryption key (raw, hex, passphrase)
        """
        cmd = ['zfs', 'create']
        
        if 'mountpoint' in kwargs:
            cmd.extend(['-o', f'mountpoint={kwargs["mountpoint"]}'])
        if 'compression' in kwargs:
            cmd.extend(['-o', f'compression={kwargs["compression"]}'])
        if 'atime' in kwargs:
            cmd.extend(['-o', f'atime={kwargs["atime"]}'])
        if 'quota' in kwargs:
            cmd.extend(['-o', f'quota={kwargs["quota"]}'])
        if 'reservation' in kwargs:
            cmd.extend(['-o', f'reservation={kwargs["reservation"]}'])
        if 'recordsize' in kwargs:
            cmd.extend(['-o', f'recordsize={kwargs["recordsize"]}'])
        if 'dedup' in kwargs:
            cmd.extend(['-o', f'dedup={kwargs["dedup"]}'])
        if 'encryption' in kwargs:
            cmd.extend(['-o', f'encryption={kwargs["encryption"]}'])
        if 'keylocation' in kwargs:
            cmd.extend(['-o', f'keylocation={kwargs["keylocation"]}'])
        if 'keyformat' in kwargs:
            cmd.extend(['-o', f'keyformat={kwargs["keyformat"]}'])

        cmd.append(name)
        super().__init__(' '.join(cmd), exec_info)


class Mount(Exec):
    """
    Mount a filesystem
    """

    def __init__(self, source, target, exec_info=None, **kwargs):
        """
        Mount a filesystem.

        :param source: Source device, partition, or network location to mount
        :param target: Target mount point directory
        :param exec_info: Information needed to execute mount
        :param kwargs: Additional mount options:
            - type: Filesystem type (e.g., ext4, xfs, btrfs, zfs, nfs, etc.)
            - options: List of mount options or string of comma-separated options
            - bind: Create a bind mount (True/False)
            - recursive: Recursively mount filesystems from fstab (True/False)
            - read_only: Mount as read-only (True/False)
            - remount: Remount an existing mount point (True/False)
            - make_dirs: Create target directory if it doesn't exist (True/False)
        """
        if kwargs.get('make_dirs', False):
            Mkdir(target, exec_info).run()

        cmd = ['mount']
        
        # Handle bind mounts
        if kwargs.get('bind', False):
            cmd.append('--bind')
        
        # Handle recursive mounting
        if kwargs.get('recursive', False):
            cmd.append('--all')
        
        # Handle filesystem type
        if 'type' in kwargs:
            cmd.extend(['-t', kwargs['type']])
        
        # Handle mount options
        options = []
        if kwargs.get('read_only', False):
            options.append('ro')
        if kwargs.get('remount', False):
            options.append('remount')
        
        # Add user-provided options
        if 'options' in kwargs:
            if isinstance(kwargs['options'], list):
                options.extend(kwargs['options'])
            else:
                options.append(kwargs['options'])
        
        if options:
            cmd.extend(['-o', ','.join(options)])
        
        cmd.extend([source, target])
        super().__init__(' '.join(cmd), exec_info)


class Umount(Exec):
    """
    Unmount a filesystem
    """

    def __init__(self, target, exec_info=None, **kwargs):
        """
        Unmount a filesystem.

        :param target: Mount point or device to unmount
        :param exec_info: Information needed to execute umount
        :param kwargs: Additional unmount options:
            - force: Force unmount (True/False)
            - lazy: Lazy unmount (True/False)
            - recursive: Recursively unmount all filesystems under target (True/False)
            - all_types: Unmount all filesystems of specified types
            - types: List of filesystem types to unmount (used with all_types)
        """
        cmd = ['umount']
        
        if kwargs.get('force', False):
            cmd.append('--force')
        
        if kwargs.get('lazy', False):
            cmd.append('--lazy')
        
        if kwargs.get('recursive', False):
            cmd.append('--recursive')
        
        if kwargs.get('all_types', False) and 'types' in kwargs:
            if isinstance(kwargs['types'], list):
                cmd.extend(['-t', ','.join(kwargs['types'])])
            else:
                cmd.extend(['-t', kwargs['types']])
        
        cmd.append(target)
        super().__init__(' '.join(cmd), exec_info)


