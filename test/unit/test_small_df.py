from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.util.hostfile import Hostfile
from jarvis_util.introspect.system_info import Lsblk, \
    ListFses, FiInfo, Blkid, ResourceGraph, StorageDeviceType
from jarvis_util.util.size_conv import SizeConv
from jarvis_util.util.small_df import SmallDf
import pathlib
import itertools
from unittest import TestCase


class TestSmallDf(TestCase):
    def test_create(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        self.assertEqual(3, len(df))
        self.assertEqual(set(['a', 'b', 'c', 'd']), set(df.columns))

    def test_query(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        sub_df = df['a']
        records = sub_df.list()
        self.assertEqual([1, None, None], records)
        sub_df = df[:, ['a', 'b']]
        records = sub_df.list()
        self.assertEqual([[1, 2], [None, None], [None, None]], records)

    def test_col_assign(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        df['a'] = 25
        records = df['a'].list()
        self.assertEqual([25, 25, 25], records)

    def test_col_row_assign(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        sub_df = df['a']
        sub_df[lambda r: r['a'] is None, 'a'] = 25
        records = set(df['a'].list())
        self.assertEqual({1, 25, 25}, records)

    def test_add(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df['a'].fillna(0)
        df['b'].fillna(0)
        df['c'] = df['a'] + df['b']
        records = set(df['c'].list())
        self.assertEqual({5, 2, 0}, records)

    def test_addeq(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df['a'].fillna(0)
        df['b'].fillna(0)
        df['a'] += df['b']
        records = set(df['a'].list())
        self.assertEqual({5, 2, 0}, records)

    def test_add2(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df['a'].fillna(0)
        df['b'].fillna(0)
        df['c'] = df['a'] + df['b'] + 5
        records = set(df['c'].list())
        self.assertEqual({10, 7, 5}, records)

    def test_mul(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df['a'].fillna(0)
        df['b'].fillna(0)
        df['c'] = df['a'] * df['b']
        records = set(df['c'].list())
        self.assertEqual({6, 0, 0}, records)

    def test_div(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df['a'].fillna(1)
        df['b'].fillna(1)
        df['c'] = df['a'] / df['b']
        records = set(df['c'].list())
        self.assertEqual({1.5, 2, 1}, records)

    def test_merge(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df1 = SmallDf(rows=rows)
        rows = [{'a': 3, 'e': 2}, {'a': 3, 'e': 4}]
        df2 = SmallDf(rows=rows)
        df3 = df1.merge(df2)
        self.assertEqual(4, len(df3))
        self.assertEqual(1, len(df3[lambda r: r['a'] == 3 and r['e'] == 2]))
        self.assertEqual(1, len(df3[lambda r: r['a'] == 3 and r['e'] == 4]))

    def test_groupby(self):
        rows = [{'a': 3, 'b': 2}, {'a': 3, 'b': 1}, {'a': 2, 'b': 4}]
        df1 = SmallDf(rows=rows)
        grp = df1.groupby('a')
        self.assertEqual(2, len(grp))
        self.assertEqual(set([tuple([2]), tuple([3])]),
                         set(grp.groups.keys()))
