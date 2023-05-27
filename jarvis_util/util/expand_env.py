import os


def expand_env(data):
    if instance(data, str):
        return os.path.expandvars(data)
    if isinstance(data, dict):
        for key, val in data.items():
            data[key] = expand_env(data[key])
    if isinstance(data, list) or isinstance(data, tuple):
        for i, val in enumerate(data):
            data[i] = expand_env(data)
    return data
