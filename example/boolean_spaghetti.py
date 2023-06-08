from jarvis_util.util.argparse import ArgParse


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_menu('spaghetti')
        self.add_args([
            {
                'name': 'cheese',
                'msg': 'Whether to use cheese',
                'type': bool,  # The type of this variable
                'default': True
            }
        ])

    def spaghetti(self):
        if self.kwargs['cheese']:
            print('I will take the spaghetti with cheese')
        else:
            print('I want actual Italian, and will not take your cheese')


args = MyArgParse()
args.process_args()
