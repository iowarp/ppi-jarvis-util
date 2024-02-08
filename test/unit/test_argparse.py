from jarvis_util.util.argparse import ArgParse
from unittest import TestCase
import shlex


class MyArgParse(ArgParse):
    def define_options(self):
        self.add_cmd(keep_remainder=True)
        self.add_args([
            {
                'name': 'hi',
                'msg': 'hello',
                'type': str,
                'default': None
            }
        ])

        self.add_cmd('vpic run',
                      keep_remainder=False,
                      aliases=['vpic r', 'vpic runner'])
        self.add_args([
            {
                'name': 'steps',
                'msg': 'Number of checkpoints',
                'type': int,
                'required': True,
                'pos': True,
                'class': 'sim',
                'rank': 0
            },
            {
                'name': 'x',
                'msg': 'The length of the x-axis',
                'type': int,
                'required': False,
                'default': 256,
                'pos': True,
                'class': 'sim',
                'rank': 1
            },
            {
                'name': 'do_io',
                'msg': 'Whether to perform I/O or not',
                'type': bool,
                'required': False,
                'default': False,
                'pos': True,
            },
            {
                'name': 'make_figures',
                'msg': 'Whether to make a figure',
                'type': bool,
                'default': False,
            },
            {
                'name': 'data_size',
                'msg': 'Total amount of data to produce',
                'type': int,
                'default': 1024,
            },
            {
                'name': 'hosts',
                'msg': 'A list of hosts',
                'type': list,
                'args': [
                    {
                        'name': 'host',
                        'msg': 'A string representing a host',
                        'type': str,
                    }
                ],
                'aliases': ['x']
            },
            {
                'name': 'devices',
                'msg': 'A list of devices and counts',
                'type': list,
                'args': [
                    {
                        'name': 'path',
                        'msg': 'The mount point of device',
                        'type': str,
                    },
                    {
                        'name': 'count',
                        'msg': 'The number of devices to search for',
                        'type': int,
                    }
                ]
            }
        ])


class TestArgparse(TestCase):
    def test_default_argparse(self):
        args = MyArgParse(args='hi=\"23528 asfda\"')
        self.assertEqual(args.kwargs['hi'], '23528 asfda')

    def test_help(self):
        MyArgParse(args='vpic run -h')

    def test_bool_kwargs(self):
        args = MyArgParse(args='vpic run 20 512 True +make_figures')
        self.assertEqual(args.kwargs['steps'], 20)
        self.assertEqual(args.kwargs['x'], 512)
        self.assertEqual(args.kwargs['do_io'], True)
        self.assertEqual(args.kwargs['make_figures'], True)
        self.assertEqual(args.kwargs['hosts'], None)
        self.assertEqual(args.kwargs['devices'], None)

    def test_bool_kwargs2(self):
        args = MyArgParse(args='vpic run 20 512 True -make_figures')
        self.assertEqual(args.kwargs['steps'], 20)
        self.assertEqual(args.kwargs['x'], 512)
        self.assertEqual(args.kwargs['do_io'], True)
        self.assertEqual(args.kwargs['make_figures'], False)
        self.assertEqual(args.kwargs['hosts'], None)
        self.assertEqual(args.kwargs['devices'], None)

    def test_bool_kwargs3(self):
        args = MyArgParse(args='vpic run 20 512 True --make_figures=true')
        self.assertEqual(args.kwargs['steps'], 20)
        self.assertEqual(args.kwargs['x'], 512)
        self.assertEqual(args.kwargs['do_io'], True)
        self.assertEqual(args.kwargs['make_figures'], True)
        self.assertEqual(args.kwargs['hosts'], None)
        self.assertEqual(args.kwargs['devices'], None)

    def test_bool_kwargs4(self):
        args = MyArgParse(args='vpic run 20 512 True --make_figures=false')
        self.assertEqual(args.kwargs['steps'], 20)
        self.assertEqual(args.kwargs['x'], 512)
        self.assertEqual(args.kwargs['do_io'], True)
        self.assertEqual(args.kwargs['make_figures'], False)
        self.assertEqual(args.kwargs['hosts'], None)
        self.assertEqual(args.kwargs['devices'], None)

    def test_list_arg(self):
        args = MyArgParse(args='vpic run 15 --hosts=\"[129.15, 1294.124]\"')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual(['129.15', '1294.124'], args.kwargs['hosts'])

    def test_list_arg2(self):
        args = MyArgParse(args='vpic run 15 --hosts=129.15 --hosts=1294.124')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual(['129.15', '1294.124'], args.kwargs['hosts'])

    def test_list_arg3(self):
        args = MyArgParse(args='vpic run 15 --hosts=129.15 --hosts=1294.124 --hosts=')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual([], args.kwargs['hosts'])

    def test_list_arg4(self):
        args = MyArgParse(args='vpic run 15 --hosts=[]')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual([], args.kwargs['hosts'])

    def test_list_list_arg(self):
        args = MyArgParse(args='vpic run 15 '
                               '--devices=\"[[nvme, 5], [sata, 25]]\"')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual([['nvme', 5], ['sata', 25]], args.kwargs['devices'])

    def test_arg_alias(self):
        args = MyArgParse(args='vpic run 15 -hosts=129.15 -hosts=1294.124')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual(['129.15', '1294.124'], args.kwargs['hosts'])

        args = MyArgParse(args='vpic run 15 -x=129.15 -x=1294.124')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual(['129.15', '1294.124'], args.kwargs['hosts'])

    def test_menu_alias(self):
        args = MyArgParse(args='vpic run 15 -hosts=129.15 -hosts=1294.124')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual(['129.15', '1294.124'], args.kwargs['hosts'])

        args = MyArgParse(args='vpic r 15 -x=129.15 -x=1294.124')
        self.assertEqual(15, args.kwargs['steps'])
        self.assertEqual(['129.15', '1294.124'], args.kwargs['hosts'])
