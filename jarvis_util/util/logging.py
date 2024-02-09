from enum import Enum

class Color(Enum):
    GREEN = '\033[92m{}\033[0m'
    RED = '\033[{}\033[0m'
    CYAN = '\033[96m{}\033[0m'

class ColorPrinter:
    @staticmethod
    def print(self, msg, color=None):
        if color is not None:
            print(color.value.format(msg))
        else:
            print(msg)
