"""
This module contains an argument parser which defines
"""

import sys
import os
from abc import ABC, abstractmethod
import shlex
import yaml
from tabulate import tabulate


class ArgParse(ABC):
    """
    A class for parsing command line arguments.
        Parsed menu name stored in self.menu_name
        Parsed menu arguments stored in self.kwargs
        Parsed remaining arguments stored in self.remainder
    """

    def __init__(self, args=None, exit_on_fail=True, **custom_info):
        """
        :param args: Unparsed CLI arguments. Either a string or a list.
        :param exit_on_fail: Whether to exit the program if parsing the
        menu fails.
        """
        if args is None:
            args = sys.argv[1:]
        elif isinstance(args, str):
            args = shlex.split(args)
        self.binary_name = os.path.basename(sys.argv[0])
        self.args = args
        self.error = None
        self.exit_on_fail = exit_on_fail
        self.custom_info = custom_info
        self.menus = []
        self.vars = {}
        self.remainder = []
        self.remainder_kv = {}
        self.pos_required = False
        self.keep_remainder = False
        self.remainder_as_kv = False

        self.needed_help = False
        self.menu = None
        self.menu_name = None
        self.kwargs = {}
        self.real_kwargs = {}
        self.define_options()
        self._parse()

    @abstractmethod
    def define_options(self):
        """
        User-defined options menu

        :return:
        """
        pass

    def process_args(self):
        """
        After args have been parsed, can call this function to process
        the arguments. Assumes that derived ArgParse class has a function
        for each menu option.

        :return: None
        """
        if self.needed_help:
            return
        func_name = self.menu_name.replace(' ', '_')
        func_name = func_name.replace('-', '_')
        if func_name == '':
            func_name = 'main_menu'
        func = getattr(self, func_name)
        func()

    def add_menu(self, name=None, msg=None,
                 keep_remainder=False,
                 remainder_as_kv=False):
        """
        A menu is a container of arguments.

        :param name: The name that appears in the CLI to trigger the menu.
        Spaces indicate menu nesting. E.g., 'repo add' will trigger the
        menu argparser only if 'repo' and 'add' appear next to each other
        in the argument list.
        :param msg: The message to print if the user selects an improper menu
        in the CLI.
        :param keep_remainder: Whether or not the menu should store all
        remaining arguments for further use later.
        :param remainder_as_kv: Automatically parse the remainder as string
        type entries.
        :return:
        """
        toks = []
        if name is not None:
            toks = name.split()
        self.menus.append({
            'name_str': ' '.join(toks),
            'name': toks,
            'msg': msg,
            'num_required': 0,
            'pos_opts': [],
            'kw_opts': {},
            'keep_remainder': keep_remainder,
            'remainder_as_kv': remainder_as_kv,
        })
        self.menu = self.menus[-1]

    @staticmethod
    def _default_arg_list_params(args):
        """
        Make the menu argument list contain all needed parameters.

        :param args: The set of arguments being modified
        :return:
        """
        for arg in args:
            if 'name' not in arg:
                raise Exception('Name is a required argument')
            if 'type' not in arg:
                arg['type'] = str
            if 'choices' not in arg:
                arg['choices'] = []
            if 'default' not in arg:
                arg['default'] = None
            if 'args' not in arg:
                arg['args'] = None
            if 'required' not in arg:
                arg['required'] = False
            if 'pos' not in arg:
                arg['pos'] = False
            if arg['args'] is not None:
                for list_args in arg['args']:
                    ArgParse._default_arg_list_params([list_args])

    def add_args(self, args):
        """
        Add arguments to the current menu

        menu arguments have the following parameters:
            name: The name of the argument
            type: The arg type (e.g., str, int, bool, list). Default str.
            choices: Available choices for the menu option. Default None.
            pos: Whether the argument is positional. Default false.
            required: Whether a positional argument is required. Default false.
            default: The default value of the argument. Default None.
            args: For arguments of the 'list' types, represents the
            meaning of entries in the list

        :param args: A list of argument dicts
        :return:
        """
        self.menu['pos_opts'] += [arg for arg in args
                                  if 'pos' in arg and
                                  arg['pos'] is True]
        self.menu['kw_opts'].update({arg['name']: arg for arg in args
                                     if 'pos' not in arg or
                                     arg['pos'] is False})
        for arg in self.menu['pos_opts']:
            if 'required' in arg and arg['required']:
                self.menu['num_required'] += 1
        self.menu['kw_opts'].update({'help': {
            'name': 'help',
            'type': bool,
            'msg': 'Print help menu',
            'default': False
        }})
        self.menu['kw_opts'].update({'h': {
            'name': 'h',
            'type': bool,
            'msg': 'Print help menu',
            'default': False
        }})
        self._default_arg_list_params(self.menu['pos_opts'])
        self._default_arg_list_params(list(self.menu['kw_opts'].values()))

    def _parse(self):
        """
        Parse the CLI arguments.
            Will modify self.menu to indicate which menu is used
            Will modify self.args to create a key-value store of arguments

        :return: None.
        """
        # Sort by longest menu length
        self.menus.sort(key=lambda x: len(x['name']), reverse=True)
        # Parse the menu options
        self._parse_menu()
        default_args = self.default_kwargs(
            list(self.menu['kw_opts'].values()) + self.menu['pos_opts'])
        default_args.update(self.kwargs)
        self.real_kwargs = self.kwargs
        self.kwargs = default_args

    @staticmethod
    def default_kwargs(menu_args):
        """
        Pack the kwargs dictionary with default values for missing entries.

        :param menu_args: The menu argument list containing all parameters
        and their defaults.
        :return: dict
        """
        kwargs = {}
        for arg in menu_args:
            if arg['name'] == 'help':
                continue
            if arg['name'] == 'h':
                continue
            if 'default' in arg:
                kwargs[arg['name']] = arg['default']
            else:
                kwargs[arg['name']] = None
        return kwargs

    def _parse_menu(self):
        """
        Determine which menu is used in the CLI.

        :return: Modify self.menu. No return value.
        """

        # Identify the menu we are currently under
        self.menu = None
        for menu in self.menus:
            menu_name = menu['name']
            if len(menu_name) > len(self.args):
                continue
            if menu_name == self.args[0:len(menu_name)]:
                self.menu = menu
                break
        if self.menu is None:
            self._invalid_menu(menu_name)
        self.menu_name = self.menu['name_str']
        menu_name = self.menu['name']
        self.keep_remainder = self.menu['keep_remainder']
        self.remainder_as_kv = self.menu['remainder_as_kv']
        self.args = self.args[len(menu_name):]
        self._parse_args()

    def _parse_args(self):
        i = self._parse_pos_args()
        self._parse_kw_args(i)
        if 'h' in self.kwargs or 'help' in self.kwargs:
            self._print_help()

    def _parse_pos_args(self):
        """
        Parse positional arguments
            Modify the self.kwargs dictionary

        :return:
        """

        i = 0
        args = self.args
        menu = self.menu
        while i < len(menu['pos_opts']):
            # Get the positional arg info
            opt_name = menu['pos_opts'][i]['name']
            if i >= len(args):
                if i >= menu['num_required']:
                    break
                else:
                    self._missing_positional(opt_name)

            # Get the arg value
            opt_val = args[i]
            if self._is_kw_value(i):
                break
            opt_val = self._convert_opt(menu['pos_opts'][i], opt_val)

            # Set the argument
            self.kwargs[opt_name] = opt_val
            i += 1
        return i

    def _parse_kw_args(self, i):
        """
        Parse key-word arguments.
            Modify the self.kwargs dictionary

        :param i: The starting index in the self.args list where kv pairs start
        :return:
        """

        menu = self.menu
        args = self.args
        while i < len(args):
            opt_name = args[i]
            opt_val = True
            if '=' in opt_name:
                opt_name, opt_val = opt_name.split('=')
            elif opt_name == '-h':
                opt_name = 'h'
                opt_val = True
            elif opt_name == '--help':
                opt_name = 'help'
                opt_val = True
            elif opt_name.startswith('+'):
                opt_val = True
            elif opt_name.startswith('-') and not opt_name.startswith('--'):
                opt_val = False

            # Normalize opt name
            opt_name = self._get_opt_name(opt_name)

            # Verify argument is apart of the menu
            if opt_name not in menu['kw_opts']:
                if self.remainder_as_kv:
                    self.remainder_kv[opt_name] = opt_val
                    i += 1
                    continue
                elif self.keep_remainder:
                    self.remainder = args[i:]
                    return
                else:
                    self._invalid_kwarg(opt_name)

            # Get argument type
            opt = menu['kw_opts'][opt_name]

            # Convert argument to type
            opt_val = self._convert_opt(opt, opt_val)

            # Set the argument
            self.kwargs[opt_name] = opt_val
            i += 1

    def _convert_opt(self, opt, arg):
        opt_name = opt['name']
        opt_type = opt['type']
        opt_choices = opt['choices']
        opt_args = opt['args']
        if opt_type is not None:
            # pylint: disable=W0702
            try:
                if opt_type is list:
                    if isinstance(arg, str):
                        arg = yaml.safe_load(arg)
                if isinstance(arg, list):
                    # Parse a list
                    # Verify each entry in the list matches opt_args
                    for i, entry in enumerate(arg):
                        if isinstance(entry, list):
                            for j, sub_entry in enumerate(entry):
                                entry[j] = self._convert_opt(opt_args[j],
                                                             sub_entry)
                        else:
                            arg[i] = self._convert_opt(opt_args[0], entry)
                elif opt_type is bool and isinstance(arg, str):
                    arg = yaml.safe_load(arg)
                else:
                    # Parse a simple type
                    if arg == '' and opt['default'] is None and not opt['required']:
                        arg = None
                    else:
                        arg = opt_type(arg)
                # Verify the opt matches the available choices
                if opt_choices is not None and len(opt_choices):
                    if arg not in opt_choices:
                        self._invalid_choice(opt_name, arg)
            except:
                self._invalid_type(opt_name, opt_type)
            # pylint: enable=W0702
        return arg

    def _is_kw_value(self, i):
        """
        Check if the argument at position i is a kwopt

        :param i:
        :return:
        """
        if i >= len(self.args):
            return False
        opt_name = self.args[i]
        if '=' in opt_name:
            return True
        if opt_name.startswith('+'):
            return True
        if opt_name.startswith('-'):
            return True
        return self._get_opt_name(opt_name) in self.menu['kw_opts']

    def _get_opt_name(self, opt_name):
        """
        Normalize option names
            '--with-' and '--no-' are removed
            '+' and '-' are removed

        :param opt_name: The menu option name
        :param is_bool_arg: Whether the arg is a boolean arg
        :return:
        """
        opt_name = opt_name.replace('--with-', '')
        opt_name = opt_name.replace('--no-', '')
        opt_name = opt_name.replace('+', '')
        opt_name = opt_name.replace('-', '')
        return opt_name

    def _invalid_menu(self, menu_name):
        self._print_error(f'Could not find a menu for {menu_name}')

    def _invalid_choice(self, opt_name, arg):
        self._print_menu_error(f'{opt_name}={arg} is not a valid choice')

    def _missing_positional(self, opt_name):
        self._print_menu_error(f'{opt_name} was required, but not defined')

    def _invalid_kwarg(self, opt_name):
        self._print_menu_error(f'{opt_name} is not a valid key-word argument')

    def _invalid_kwarg_default(self, opt_name):
        self._print_menu_error(
            f'{opt_name} was not given a value, but requires one')

    def _invalid_type(self, opt_name, opt_type):
        self._print_menu_error(f'{opt_name} was not of type {opt_type}')

    def _print_menu_error(self, msg):
        self._print_error(f'In the menu {self.menu["name_str"]}, {msg}')

    def _print_error(self, msg):
        print(f'{msg}')
        self._print_help()
        if self.exit_on_fail:
            sys.exit(1)
        else:
            raise Exception(msg)

    def _print_help(self):
        self.needed_help = True
        if self.menu is not None:
            self._print_menu_help()
        else:
            self._print_menus()

    def _print_menus(self):
        for menu in self.menus:
            self.menu = menu
            self._print_menu_help(True)

    def _print_menu_help(self, only_usage=False):
        pos_args = []
        for arg in self.menu['pos_opts']:
            if arg['required']:
                pos_args.append(f'[{arg["name"]}]')
            else:
                pos_args.append(f'[{arg["name"]} (opt)]')
        pos_args = ' '.join(pos_args)
        menu_str = self.menu['name_str']
        if len(self.menu['kw_opts']):
            print(f'USAGE: {self.binary_name} {menu_str} {pos_args} ...')
        else:
            print(f'USAGE: {self.binary_name} {menu_str} {pos_args}')
        if self.menu['msg'] is not None:
            print(self.menu['msg'])
        print()
        if only_usage:
            return

        headers = ['Name', 'Default', 'Type', 'Description']
        table = []
        all_opts = self.menu['pos_opts'] + list(self.menu['kw_opts'].values())
        for arg in all_opts:
            default = arg['default'] if 'default' in arg else None
            table.append(
                [arg['name'], default, self._get_type(arg), arg['msg']])
        print(tabulate(table, headers=headers))

    def _get_type(self, arg):
        if arg['type'] == list:
            return str([self._get_type(subarg) for subarg in arg['args']])
        else:
            return str(arg['type']).split("'")[1]
