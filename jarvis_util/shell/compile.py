
from .exec import Exec
from .local_exec import LocalExecInfo
from .filesystem import Mkdir

class Cmake(Exec):
    def __init__(self, root_dir, out_dir, opts=None, exec_info=None):
        """
        Run cmake

        :param root_dir: Where the root of the cmake project is
        :param out_dir: Where to output build data
        :param opts: A dict mapping cmake keys to values
        :param exec_info: The execution info
        """
        if exec_info is None:
            exec_info = LocalExecInfo()
        self.opts = opts
        self.root_dir = root_dir
        self.out_dir = out_dir
        Mkdir(out_dir)
        self.cmd = [f'cmake {root_dir}']
        if opts is not None:
            for key, val in opts.items():
                if isinstance(val, bool):
                    if val:
                        self.cmd.append(f'-D{key}=ON')
                    else:
                        self.cmd.append(f'-D{key}=OFF')
                else:
                    self.cmd.append(f'-D{key}={val}')
        self.cmd = ' '.join(self.cmd)
        super().__init__(self.cmd, exec_info.mod(cwd=self.out_dir))

class Make(Exec):
    def __init__(selfs, build_dir, nthreads=8, install=False,
                 exec_info=None):
        if exec_info is None:
            exec_info = LocalExecInfo()
        if install:
            cmd = f'make -j{nthreads} install'
        else:
            cmd = f'make -j{nthreads}'
        super().__init__(cmd,
                         exec_info.mod(cwd=build_dir))
