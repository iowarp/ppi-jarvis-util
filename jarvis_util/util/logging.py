from enum import Enum

class Color(Enum):
    BLACK = "\033[30m{}\033[0m"
    RED = "\033[31m{}\033[0m"
    GREEN = "\033[32m{}\033[0m"
    YELLOW = "\033[33m{}\033[0m"
    BLUE = "\033[34m{}\033[0m"
    MAGENTA = "\033[35m{}\033[0m"
    CYAN = "\033[36m{}\033[0m"
    WHITE = "\033[37m{}\033[0m"
    BRIGHT_BLACK = "\033[90m{}\033[0m"
    BRIGHT_RED = "\033[91m{}\033[0m"
    BRIGHT_GREEN = "\033[92m{}\033[0m"
    BRIGHT_YELLOW = "\033[93m{}\033[0m"
    BRIGHT_BLUE = "\033[94m{}\033[0m"
    BRIGHT_MAGENTA = "\033[95m{}\033[0m"
    BRIGHT_CYAN = "\033[96m{}\033[0m"
    BRIGHT_WHITE = "\033[97m{}\033[0m"

class ColorPrinter:
    @staticmethod
    def print(msg, color=None):
        if color is not None:
            print(color.value.format(msg))
        else:
            print(msg)
