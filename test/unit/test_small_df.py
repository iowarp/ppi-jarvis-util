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
        self.assertTrue(len(df) == 3)
        self.assertEqual(df.columns, set(['a', 'b', 'c', 'd']))

    def test_query(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        sub_df = df['a']
        records = [row['a'] for row in sub_df.rows]
        self.assertTrue(records == [1, None, None])

    def test_col_assign(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        sub_df = df['a']
        records = [row['a'] for row in sub_df.rows]
        sub_df['a'] = 25
        records = [row['a'] for row in sub_df.rows]
        self.assertTrue(records == [25, 25, 25])

    def test_col_row_assign(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        sub_df = df['a']
        sub_df[lambda r: r['a'] is None, 'a'] = 25
        records = [row['a'] for row in sub_df.rows]
        self.assertTrue(records == [1, 25, 25])
        records = [row['a'] for row in df.rows]
        self.assertTrue(records == [1, None, None])

    def test_loc(self):
        rows = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        df = SmallDf(rows=rows)
        sub_df = df.loc['a']
        sub_df[lambda r: r['a'] is None, 'a'] = 25
        records = [row['a'] for row in sub_df.rows]
        self.assertEqual(records, [1, 25, 25])
        records = [row['a'] for row in df.rows]
        self.assertEqual(records, [1, 25, 25])

    def test_add(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df.loc['a'].fillna(0)
        df.loc['b'].fillna(0)
        df.loc['c'] = df['a'] + df['b']
        records = [row['c'] for row in df.rows]
        self.assertEqual(records, [5, 2, 0])

    def test_add2(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df.loc['a'].fillna(0)
        df.loc['b'].fillna(0)
        df.loc['c'] = df['a'] + df['b'] + 5
        records = [row['c'] for row in df.rows]
        self.assertEqual(records, [10, 7, 5])

    def test_mul(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df.loc['a'].fillna(0)
        df.loc['b'].fillna(0)
        df.loc['c'] = df['a'] * df['b']
        records = [row['c'] for row in df.rows]
        self.assertEqual(records, [6, 0, 0])

    def test_div(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df = SmallDf(rows=rows)
        df.loc['a'].fillna(1)
        df.loc['b'].fillna(1)
        df.loc['c'] = df['a'] / df['b']
        records = [row['c'] for row in df.rows]
        self.assertEqual(records, [1.5, 2, 1])

    def test_merge(self):
        rows = [{'a': 3, 'b': 2}, {'a': 2}, {'d': 4}]
        df1 = SmallDf(rows=rows)
        rows = [{'a': 3, 'e': 2}, {'a': 3, 'e': 4}]
        df2 = SmallDf(rows=rows)
        df3 = df1.merge(df2)
        self.assertEqual(len(df3), 5)
        self.assertEqual(len(df3[lambda r: r['a'] == 3 and r['e'] == 2]), 1)
        self.assertEqual(len(df3[lambda r: r['a'] == 3 and r['e'] == 4]), 1)
