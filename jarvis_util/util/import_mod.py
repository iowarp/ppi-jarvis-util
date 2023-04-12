
def load_class(import_str, path, class_name):
    """
    Loads a class from a python file.

    :param import_str: A python import string
    :param path: The absolute path to the file
    :param class_name: The name of the class in the file
    :return:
    """
    sys.path.insert(0, repo_path)
    module = __import__(import_str, fromlist=[class_name])
    sys.path.pop(0)
    return getattr(module, class_name)
