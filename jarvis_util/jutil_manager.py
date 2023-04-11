
class JutilManager:
    instance_ = None

    @staticmethod
    def get_instance():
        if JutilManager.instance_ is None:
            instance_ = JutilManager()
        return instance_

    def __init__(self):
        self.collect_output = True
