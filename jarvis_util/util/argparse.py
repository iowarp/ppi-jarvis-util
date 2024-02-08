"""
This module contains an argument parser which defines
"""

import sys
import os
from abc import ABC, abstractmethod
import shlex
import yaml
from tabulate import tabulate


class MenuSequence:
    def __init__(self, alias_toks):
        self.alias = alias_toks
        self.hash = 0
        for tok in alias_toks:
            self.hash += hash(tok)

    def __hash__(self):
        return self.hash

    def __eq__(self, other):
        return other.alias == self.alias


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
        self.menu_alias = None
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

    def _get_alias(self, name):
        if name is not None:
            name_toks = name.split()
            name_str = ' '.join(name_toks)
            return (name_str, name_toks)
        return ('', [])

    def add_cmd(self, name=None, msg=None,
                 keep_remainder=False,
                 remainder_as_kv=False,
                 aliases=None):
        self.add_menu(name, msg,
                      keep_remainder,
                      remainder_as_kv,
                      aliases,
                      is_cmd=True)

    def add_menu(self, name=None, msg=None,
                 keep_remainder=False,
                 remainder_as_kv=False,
                 aliases=None,
                 is_cmd=False):
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
        :param rank: rank the importance of this argument to print
        :param aliases: Alternative names for this menu
        :param is_cmd: This menu represents a single command
        :return:
        """
        toks = []
        name_str, name_toks = self._get_alias(name)
        full_aliases = []
        if aliases is None:
            aliases = []
        for alias in aliases:
            full_aliases.append(self._get_alias(alias))
        full_aliases.append((name_str, name_toks))
        menu = {
                'name_str': name_str,
                'name_toks': name_toks,
                'msg': msg,
                'num_required': 0,
                'pos_opts': [],
                'kw_opts': {},
                'keep_remainder': keep_remainder,
                'remainder_as_kv': remainder_as_kv,
                'is_cmd': is_cmd,
            }
        for alias in full_aliases:
            self.menus.append((alias, menu))
        self.menu = self.menus[-1][1]

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
            rank: The importance of this argument to print
            class: The category of option
            aliases: Alternative names for keyword arguments

        :param args: A list of argument dicts
        :return:
        """
        self.menu['pos_opts'] += [arg for arg in args
                                  if 'pos' in arg and
                                  arg['pos'] is True]
        self.menu['kw_opts'].update({arg['name']: arg for arg in args
                                     if 'pos' not in arg or
                                     arg['pos'] is False})
        for opt in self.menu['pos_opts']:
            if 'required' in opt and opt['required']:
                self.menu['num_required'] += 1
            if 'rank' not in opt:
                opt['rank'] = len(args)
            if 'class' not in opt:
                opt['class'] = None
        self.menu['kw_opts'].update({'help': {
            'name': 'help',
            'type': bool,
            'msg': 'Print help menu',
            'default': False,
            'aliases': ['h']
        }})
        for opt_name, opt in list(self.menu['kw_opts'].items()):
            if 'rank' not in opt:
                opt['rank'] = len(args)
            if 'class' not in opt:
                opt['class'] = None
            if 'aliases' in opt:
                for alias in opt['aliases']:
                    self.menu['kw_opts'][alias] = opt
        self._default_arg_list_params(self.menu['pos_opts'])
        self._default_arg_list_params(list(self.menu['kw_opts'].values()))

    def _parse(self):
        """
        Parse the CLI arguments.
            Will modify self.menu to indicate which menu is used
            Will modify self.args to create a key-value store of arguments

        :return: None.
        """
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

        # Sort by longest menu length
        self.menus.sort(key=lambda x: len(x[0][1]), reverse=True)
        # Identify the menu we are currently under
        self.menu = None
        for alias, menu in self.menus:
            menu_name_toks = alias[1]
            if len(menu_name_toks) > len(self.args):
                continue
            if not menu['is_cmd']:
                continue
            if menu_name_toks == self.args[0:len(menu_name_toks)]:
                self.menu = menu
                self.menu_alias = alias
                self.args = self.args[len(menu_name_toks):]
                break
        if self.menu is None:
            self._invalid_menu(self.args)
        self.menu_name = self.menu['name_str']
        self.keep_remainder = self.menu['keep_remainder']
        self.remainder_as_kv = self.menu['remainder_as_kv']
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
            opt = menu['pos_opts'][i]
            opt_val = self._convert_opt(opt, opt_val)

            # Set the argument
            self._set_opt(opt, opt_val)
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
            self._set_opt(opt, opt_val)
            i += 1

    def _set_opt(self, opt, opt_val):
        opt_name = opt['name']
        if isinstance(opt_val, list):
            if opt_name not in self.kwargs or len(opt_val) == 0:
                self.kwargs[opt_name] = opt_val
            else:
                self.kwargs[opt_name] += opt_val
        else:
            self.kwargs[opt_name] = opt_val

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
                    if not isinstance(arg, list):
                        if arg is None:
                            arg = []
                        elif isinstance(arg, str) and len(arg) == 0:
                            arg = []
                        else:
                            arg = [arg]
                if isinstance(arg, list):
                    # Parse a list
                    # Verify each entry in the list matches opt_args
                    for i, entry in enumerate(arg):
                        if isinstance(entry, list):
                            for j, sub_entry in enumerate(entry):
                                entry[j] = self._convert_opt(opt_args[j],
                                                             sub_entry)
                        else:
                            arg[i] = self._convert_opt(
                                opt_args[0], entry)
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
        self._print_error(f'Could not find a menu for {menu_name}',
                          bad_menu=menu_name)

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

    def _print_error(self, msg,
                     bad_menu=None):
        print(f'{msg}')
        self._print_help(bad_menu)
        if self.exit_on_fail:
            sys.exit(1)
        else:
            raise Exception(msg)

    def _print_help(self,
                    bad_menu=None):
        self.needed_help = True
        if self.menu is not None:
            self._print_menu_help()
        else:
            self._print_menus(bad_menu)

    def _lcs(self, bad_menu):
        menus = {MenuSequence(alias[1]): len(alias[1])
                 for alias, menu in self.menus}
        for i in range(len(bad_menu)):
            seq = MenuSequence(bad_menu[0:i+1])
            if seq not in menus:
                return i
        return 1

    def _print_menus(self, bad_menu):
        # Longest matching subsequence
        lcs_len = self._lcs(bad_menu)
        menus = list(self.menus)
        menus.sort(key=lambda x: len(x[0][1]))
        if len(bad_menu) == 0:
            for alias, menu in menus:
                self.menu = menu
                self.menu_alias = alias
                if len(alias[1]) != 1:
                    continue
                self._print_menu_help(True)
        else:
            for alias, menu in menus:
                self.menu = menu
                self.menu_alias = alias
                if len(alias[1]) < lcs_len or len(alias[1]) > lcs_len + 2:
                    continue
                if alias[1][0:lcs_len] == bad_menu[0:lcs_len]:
                    self._print_menu_help(True)

    def _print_menu_help(self, only_usage=False):
        if not self.menu['is_cmd']:
            print(f'MENU: {self.binary_name} {self.menu_alias[0]}')
            print(self.menu['msg'])
            print()
            return

        # Print usage menu
        pos_args = []
        for arg in self.menu['pos_opts']:
            if arg['required']:
                pos_args.append(f'[{arg["name"]}]')
            else:
                pos_args.append(f'[{arg["name"]} (opt)]')
        pos_args = ' '.join(pos_args)
        if self.menu is not None:
            menu_str = self.menu_alias[0]
        if len(self.menu['kw_opts']):
            print(f'COMMAND: {self.binary_name} {menu_str} {pos_args} ...')
        else:
            print(f'COMMAND: {self.binary_name} {menu_str} {pos_args}')
        if self.menu['msg'] is not None:
            print(self.menu['msg'])
        if self.menu['name_str'] != self.menu_alias[0]:
            print(f'This is an alias to {self.menu["name_str"]}')
        print()
        if only_usage:
            return

        # Get pos + kw opts and filter out aliases
        all_opts = (self.menu['pos_opts'] +
                    [opt
                     for name, opt in self.menu['kw_opts'].items()
                     if name == opt['name']])

        # Group options into classes
        all_class_opts = {}
        for opt in all_opts:
            if opt['class'] is None:
                opt['class'] = ''
            if opt['class'] not in all_class_opts:
                all_class_opts[opt['class']] = [opt]
            else:
                all_class_opts[opt['class']] += [opt]
        all_class_opts = list(all_class_opts.items())
        all_class_opts.sort()

        # Print each option class
        headers = ['Name', 'Default', 'Type', 'Description']
        for class_name, class_opts in all_class_opts:
            table = []
            print(f'Option Class: {class_name}')
            class_opts.sort(key=lambda opt: opt['rank'])
            for arg in class_opts:
                default = arg['default'] if 'default' in arg else None
                names = [arg['name']]
                if 'aliases' in arg:
                    names += list(arg['aliases'])
                names.sort()
                name_set = ','.join(names)
                table.append(
                    [name_set, default, self._get_type(arg), arg['msg']])
            print(tabulate(table, headers=headers))
            print()

    def _get_type(self, arg):
        if arg['type'] == list:
            return str([self._get_type(subarg) for subarg in arg['args']])
        else:
            return str(arg['type']).split("'")[1]
