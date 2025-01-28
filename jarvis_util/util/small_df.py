"""
This module provides a simple database implementation which stored
and saved in a human-readable format.
"""
from jarvis_util.serialize.yaml_file import YamlFile
# from jarvis_util.util.import_mod import load_class
import copy
import yaml


class SmallDf:
    """
    This class provides a simple database implementation which stored
    and saved in a human-readable format.

    :param rows: List[Dict] of entries
    :param columns: List or string of columns
    destroying columns by accident
    """
    def __init__(self, rows=None, columns=None):
        self.rows = []
        self.columns = []
        if columns is not None:
            self.set_columns(columns)
        if rows is not None:
            self.concat(rows)
        if columns is None:
            self.infer_columns()

    def concat(self, df):
        """
        Concatenate a dataframe (or records) to this one
        """
        if len(df) == 0:
            return self
        if isinstance(df, SmallDf):
            self.rows += df.rows
            self.add_columns(df.columns)
        elif isinstance(df, list):
            rows = df
            if not isinstance(rows[0], dict):
                rows = [{col: row[i] for i, col in enumerate(self.columns)}
                        for row in rows]
            self.rows += rows
        self._correct_rows()
        return self

    def drop_duplicates(self):
        """
        Remove duplicate entries
        Modifies in place
        """
        dedup = self._drop_duplicates(self.rows)
        self.rows.clear()
        self.rows += dedup
        return self

    def _drop_duplicates(self, rows):
        dedup = list(set(self._fixed_dict(rows)))
        return self._mutable_dict(dedup)

    def _fixed_dict(self, rows):
        return tuple(tuple((key, row[key]) for key in self.columns) for row in rows)

    def _mutable_dict(self, rows):
        # return [{key:val for key, val in row} for row in rows]
        return [dict(row) for row in rows]

    def set_columns(self, columns):
        """
        Define the set of columns manually, without intrsopection

        :param columns: the list of columns
        :return: self
        """
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        self.columns = columns
        self._correct_rows()
        return self

    def infer_columns(self, rows=None):
        """
        Infer columns based on the rows

        :return: None
        """
        if rows is None:
            rows = self.rows
        for row in rows:
            self.add_columns(list(row.keys()))

    def add_columns(self, columns):
        """
        Add columns to the table. New columns will be appended to the
        column list.

        :param columns: the set of columns to add
        :return: self
        """
        if columns is None:
            return self
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        new_cols = [col for col in columns if col not in self.columns]
        self.columns += new_cols
        self._correct_rows()
        return self

    def drop_columns(self, columns):
        """
        Remove columns from the table

        :param columns: The columns to remove
        :return: self
        """
        if not isinstance(columns, (list, tuple, set)):
            columns = [columns]
        if len(columns) == 0:
            return
        self.columns = [col for col in self.columns if col not in columns]
        self._correct_rows()
        return self

    def rename(self, columns):
        """
        Rename a set of columns

        :param columns: New column names. Dict[OrigName, NewName]
        :return: self
        """
        for i, col in enumerate(self.columns):
            if col in columns:
                self.columns[i] = col
        for row in self.rows:
            for old_name, new_name in columns.items():
                row[new_name] = row.pop(old_name)
        return self

    def merge(self, other, on=None):
        """
        Merge this dictionary with another
        Returns a copy of the dataframe

        :param other: The other SmallDf
        :param on: The set of columns to merge on
        :return: SmallDf
        """
        if on is None:
            on = set(self.columns) & set(other.columns)
        if len(on) == 0:
            return SmallDf()
        rows = []
        for row in self.rows:
            for orow in other.rows:
                if all(row[col] == orow[col] for col in on):
                    merge_row = {}
                    merge_row.update(copy.deepcopy(row))
                    merge_row.update(copy.deepcopy(orow))
                    rows.append(merge_row)
                    orow['$#matched'] = True
                    row['$#matched'] = True
        rows += self._find_unmatched(self.rows)
        rows += self._find_unmatched(other.rows)
        for row in rows:
            if '$#matched' in row:
                del row['$#matched']
            else:
                for col in self.columns:
                    if col not in row:
                        row[col] = None
        return SmallDf(rows=rows)

    def _find_unmatched(self, orig_rows):
        unmatched = []
        for row in orig_rows:
            if '$#matched' not in row:
                unmatched.append(row)
        return unmatched

    def match(self, func):
        """
        Identify a subset of rows matching the query

        :param func: A function which takes as input a row and returns bool
        :return: a list of booleans
        """
        return [func(row) for row in self.rows]

    def loc(self, *idxer):
        """
        Identify a subset of rows
        A subset of the dataset is returned
        Values of the original dataframe can be modified

        :param idxer: A row, col selector
        :return: SmallDf
        """
        func, columns = self._query_args(*idxer)
        rows = self.rows
        if func is not None:
            rows = [row for row in rows if func(row)]
        self.add_columns(columns)
        if columns is None:
            columns = self.columns
        df = SmallDf(rows=rows, columns=columns)
        return df

    def _query_args(self, *idxer):
        """
        Parse arguments for querying

        :param idxer: An indexer tuple
        :return:
        """
        if len(idxer) == 1:
            idxer = idxer[0]
            if callable(idxer):
                return idxer, None
            elif isinstance(idxer, (list, tuple, str)):
                return None, idxer
            elif isinstance(idxer, slice):
                return None, None
        if len(idxer) == 2:
            if isinstance(idxer[1], (list, tuple, str)):
                columns = idxer[1]
            elif isinstance(idxer[1], slice):
                columns = None
            else:
                raise Exception('Invlaid parameters to loc')
            if callable(idxer[0]):
                func = idxer[0]
            elif isinstance(idxer[0], slice):
                func = None
            else:
                raise Exception('Invlaid parameters to loc')
            return func, columns
        raise Exception('Invlaid parameters to loc')

    def apply(self, func):
        """
        Apply a function to all rows
        Modifies the dataframe in-place.

        :param func: A lambda which takes as input row + col
        :return: None
        """
        for row in self.rows:
            for col in self.columns:
                row[col] = func(row, col)
        return self

    def fillna(self, val):
        """
        Fill None values with a new value

        :param val: The new default value
        :return: self
        """
        self.apply(lambda r, c: val if r[c] is None else r[c])
        return self

    def unique(self):
        """
        Get unique values

        :return: SmallDf
        """
        df = self.copy()
        df.drop_duplicates()
        return df

    def list(self):
        """
        Convert dataframe to a list of record values
        :return: List of records
        """
        if len(self.columns) > 1:
            return [[row[col] for col in self.columns] for row in self.rows]
        elif len(self.columns) == 1:
            col = list(self.columns)[0]
            return [row[col] for row in self.rows]
        else:
            return []

    def sort_values(self, col):
        """
        Sort the dataframe by a column

        :param col: The column to sort by
        :return: self
        """
        self.rows.sort(key=lambda x: x[col])
        return self

    def groupby(self, columns):
        """
        Group by a combo of columns

        :param columns: the set of columns to group by
        :return: SmallGroupBy
        """
        # smallgrpby = load_class('jarvis_util.util.small_df',
        # '', 'SmallGroupBy')
        return SmallGroupBy(columns, self.rows)

    def __getitem__(self, idxer):
        """
        Analagous to loc

        :param idxer: A row, col selector
        :return: SmallDf
        """
        if isinstance(idxer, tuple):
            return self.loc(*idxer)
        else:
            return self.loc(idxer)

    def __setitem__(self, idxer, other):
        """
        Subsets the dataset and then assigns a value to the subset

        :param idxer: A row, col selector
        :return: SmallDf
        """
        if isinstance(idxer, tuple):
            df = self.loc(*idxer)
        else:
            df = self.loc(idxer)
        if isinstance(other, SmallDf):
            if len(df.rows) != len(other.rows):
                raise Exception('Number of rows in dfs different')
            if len(df.columns) != len(other.columns):
                raise Exception('Column names do not match')
            for row, orow in zip(df.rows, other.rows):
                for col, ocol in zip(df.columns, other.columns):
                    row[col] = orow[ocol]
        else:
            for row in df.rows:
                for col in df.columns:
                    row[col] = other

    def _op(self, other, func):
        """
        Apply an arithmetic op

        :param other: Other SmallDf
        :param func: The operation to perform
        :return: SmallDf
        """
        if isinstance(other, SmallDf):
            if len(self.rows) != len(other.rows):
                raise Exception('Number of rows in dfs different')
            if len(self.columns) != len(other.columns):
                raise Exception('Column names do not match')
            rows = [{col: func(row, col, orow, ocol)
                     for col, ocol in zip(self.columns, other.columns)}
                    for row, orow in zip(self.rows, other.rows)]
        else:
            rows = [{col: row[col] + other for col in self.columns}
                    for row in self.rows]
        return SmallDf(rows=rows, columns=self.columns)

    def _opeq(self, other, func):
        """
        Apply an arithmetic op in-place

        :param other: Other SmallDf
        :param func: The operation to perform
        :return: SmallDf
        """
        df = self._op(other, func)
        for row, orow in zip(self.rows, df.rows):
            for col in df.columns:
                row[col] = orow[col]
        return self
    
    def __contains__(self, row):
        """
        Check if a row is in the dataframe

        :param row: The row to check
        :return: bool
        """
        return row in self.rows

    def __add__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] + orow[ocol])

    def __iadd__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    def __sub__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] - orow[ocol])

    def __isub__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    def __mul__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] * orow[ocol])

    def __imul__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    def __truediv__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] / orow[ocol])

    def __itruediv__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    def __len__(self):
        """
        Length of this df (# rows)

        :return: int
        """
        return len(self.rows)

    def _correct_rows(self):
        """
        Ensure that all rows have the same columns
        :return: None
        """
        for row in self.rows:
            self._correct_row(row)

    def _correct_row(self, row):
        """
        Ensure that a particular row has all columns

        :param row: The row to correct (Dict)
        :return: None
        """
        for col in self.columns:
            if col not in row:
                row[col] = None

    def to_yaml(self, path):
        """
        Save to YAML

        :param path: Output path
        :return:
        """
        YamlFile(path).save(self.rows)

    def load_yaml(self, path):
        """
        Load from YAML

        :param path: The input YAML file
        :return:
        """
        self.rows = YamlFile(path).load()

    def to_string(self):
        return yaml.dump(self.copy().rows)

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        return self.to_string()

    def copy(self):
        # rows = [{col: row[col] for col in self.columns} for row in self.rows]
        rows = [[row[col] for col in self.columns ] for row in self.rows]
        df = SmallDf(rows=rows, columns=self.columns)
        return df


def concat(dfs):
    """
    Concat a list of dfs

    :param dfs: A list or single SmallD
    :return: SmallDf
    """
    if dfs is None:
        return
    if not isinstance(dfs, (list, tuple, set)):
        dfs = [dfs]
    if len(dfs) < 1:
        return
    new_df = SmallDf()
    for df in dfs:
        new_df = new_df.concat(df)
    return new_df


def merge(dfs, on=None, how=None):
    """
    Merge a set of dfs

    :param dfs: A list of dataframes to merge
    :param on: The columns to merge on
    :param how: The merge type. Only None & outer is supported
    :return: SmallDf
    """
    if how is not None and how != 'outer':
        raise Exception('Only outer merge supported')
    if dfs is None:
        return
    if not isinstance(dfs, (list, tuple, set)):
        dfs = [dfs]
    if len(dfs) <= 1:
        return
    base_df = dfs[0]
    for df in dfs[1:]:
        base_df = base_df.merge(df, on=on)
    return base_df


class SmallGroupBy:
    """
    This class groups a df based on columns
    """
    def __init__(self, columns=None, rows=None):
        self.groups = {}
        self.columns = []
        if columns is None and rows is None:
            return
        if isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = columns
        for row in rows:
            key = tuple(row[col] for col in self.columns)
            if key not in self.groups:
                self.groups[key] = []
            self.groups[key].append(row)
        for key in self.groups:
            self.groups[key] = SmallDf(rows=self.groups[key])

    def reset_index(self):
        """
        Expand the groupby into a SmallDf

        :return: None
        """
        rows = []
        for grp_df in self.groups.values():
            rows += grp_df.rows
        return SmallDf(rows=rows)

    def filter(self, func):
        """
        Keep only elements meeting the condition

        :param func: A function which takes as input a row and returns bool
        :return: SmallGroupBy
        """
        grp = SmallGroupBy()
        for key, grp_df in self.groups.items():
            grp.groups[key] = SmallDf(
                rows=[row for row in grp_df.rows if func(row)])
        return grp

    def filter_groups(self, func):
        """
        Keep only groups meeting the condition

        :param func: A function which takes as input SmallDf and returns bool
        :return: SmallGroupBy
        """
        grp = SmallGroupBy()
        for key, grp_df in self.groups.items():
            if func(grp_df):
                grp.groups[key] = grp_df
        return grp

    def first(self):
        """
        Get the first element in each group

        :return: SmallGroupBy
        """
        return self.head(1)

    def head(self, n):
        """
        Get the first "n" elements in each group

        :param n: number of elements per-group
        :return:
        """
        grp = SmallGroupBy()
        for key, grp_df in self.groups.items():
            grp.groups[key] = SmallDf(rows=grp_df.rows[0:n])
        return grp

    def __len__(self):
        """
        Get the number of groups

        :return: Number of groups (int)
        """
        return len(self.groups)
