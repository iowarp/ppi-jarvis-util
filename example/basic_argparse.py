from jarvis_util.util.argparse import ArgParse


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_menu()
        self.add_args([
            {
                'name': 'hello',
                'msg': 'A message to print',
                'type': str,  # The type of this variable
                'required': True,  # This argument is required
                'pos': True,  # This is a positional argument
            },
            {
                'name': 'hello_optional',
                'msg': 'An optional message to print',
                'type': str,  # The type of the variable to produce
                'default': 'no optional message given',
                'required': False,  # This argument is not required
                'pos': True,  # This is a positional argument
            },
            {
                'name': 'hello_kwarg',
                'msg': 'An integer key-word argument to print',
                'type': int,  # The type of the variable
                'default': 0,
            },
        ])

    # When add_menu has no parameters, process_args will call this function
    def main_menu(self):
        # Parsed parameters are placed in self.kwargs
        print(self.kwargs['hello'])
        print(self.kwargs['hello_optional'])
        print(self.kwargs['hello_kwarg'])
        print(self.kwargs)
        print(self.real_kwargs)


args = MyArgParse()
args.process_args()
