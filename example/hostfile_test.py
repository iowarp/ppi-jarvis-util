from jarvis_util.util.argparse import ArgParse


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_menu('vpic run',
                      keep_remainder=False)
        self.add_args([
            {
                'name': 'hosts',
                'msg': 'A list of hosts and threads pr',
                'type': list,
                'args': [
                    {
                        'name': 'host',
                        'msg': 'A string representing a host',
                        'type': str,
                    }
                ]
            }
        ])

    def vpic_run(self):
        print(self.kwargs['hosts'])


args = MyArgParse()
args.process_args()
