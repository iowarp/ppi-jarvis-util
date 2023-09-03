from jarvis_util.util.argparse import ArgParse


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_menu(keep_remainder=True,
                      remainder_as_kv=True)
        self.add_args([
            {
                'name': 'hi',
                'msg': 'hello',
                'type': str,
                'default': None
            }
        ])

    def main_menu(self):
        print(self.kwargs['hi'])
        print(self.remainder_kv)


args = MyArgParse()
args.process_args()
