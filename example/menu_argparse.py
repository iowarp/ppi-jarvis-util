from jarvis_util.util.argparse import ArgParse


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_menu('vpic')
        self.add_args([
            {
                'name': 'steps',
                'msg': 'Number of execution steps',
                'type': int,  # The type of this variable
                'required': True,  # This argument is required
                'pos': True,  # This is a positional argument
            }
        ])

        self.add_menu('bd-cats run')
        self.add_args([
            {
                'name': 'path',
                'msg': 'Path to particle data',
                'type': str,  # The type of this variable
                'required': True,  # This argument is required
                'pos': True,  # This is a positional argument
            }
        ])

        self.add_menu('bd-cats draw')
        self.add_args([
            {
                'name': 'resolution',
                'msg': 'Dimensions of the image to create',
                'type': str,  # The type of this variable
                'required': True,  # This argument is required
                'pos': True,  # This is a positional argument
            }
        ])

    def vpic(self):
        print(f'Starting VPIC with {self.kwargs["steps"]} steps')

    def bd_cats_run(self):
        print(f'Starting BD-CATS with {self.kwargs["path"]}')

    def bd_cats_draw(self):
        print(f'Drawing BD-CATS output at {self.kwargs["resolution"]}')


args = MyArgParse()
args.process_args()
