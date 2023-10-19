from jarvis_util.util.argparse import ArgParse


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_menu()
        self.add_args([
            {
                'name': 'hi',
                'msg': 'hello',
                'type': str,
                'choices': ['a', 'b', 'c'],
                'default': None
            }
        ])

    def main_menu(self):
        print(self.kwargs['hi'])


args = MyArgParse()
args.process_args()
