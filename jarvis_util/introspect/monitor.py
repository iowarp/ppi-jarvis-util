class Callgrind(Exec):
    def __init__(self, cmd, exec_info=None):
        super().__init__(f'valgrind --tool=callgrind {cmd}')

