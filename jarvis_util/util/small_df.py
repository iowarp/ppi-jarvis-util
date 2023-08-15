"""
This module provides a simple database implementation which stored
and saved in a human-readable format.
"""
from jarvis_util.serialize.yaml_file import YamlFile
from collections.abc import Iterable
import copy


class SmallDf:
    """
    This class provides a simple database implementation which stored
    and saved in a human-readable format.

    :param rows: List[Dict] of entries
    :param cols: List or string of columns
    :param is_loc: Is this df being used for indexing? This will avoid
    destroying columns by accident
    """
    def __init__(self, rows=None, cols=None, is_loc=False):
        self.rows = []
        self.columns = set()
        self.dtypes = []
        self.is_loc = is_loc
        if rows is not None:
            self.concat(rows)
        if cols is not None:
            self.set_cols(cols)
        if not is_loc:
            self.loc = SmallDf(is_loc=True)
            self.loc.columns = self.rows
            self.loc.rows = self.rows

    """
    Concatenate a dataframe (or records) to this one
    """
    def concat(self, df):
        if isinstance(df, SmallDf):
            self.rows += df.rows
        elif isinstance(df, list):
            self.rows += df
            for row in df:
                self.columns.update(row.keys())
        self._correct_rows()

    """
    Set the columns
    """
    def set_cols(self, cols):
        if not isinstance(cols, Iterable):
            cols = [cols]
        self.columns.clear()
        self.columns.update(cols)
        if not self.is_loc:
            self._correct_rows()

    """
    Add columns to the table
    """
    def add_cols(self, cols):
        if self.is_loc:
            raise Exception("Cannot add columns to location df")
        if not isinstance(cols, Iterable):
            cols = [cols]
        if not any([col in self.columns for col in cols]):
            return
        self.columns.update(cols)
        self._correct_rows()

    """
    Remove columns from the table
    """
    def rm_cols(self, cols):
        if self.is_loc:
            raise Exception("Cannot remove columns from location df")
        if not isinstance(cols, Iterable):
            cols = [cols]
        if len(cols) == 0:
            return
        self.columns.difference_update(cols)
        self._correct_rows()

    """
    Merge this dictionary with another
    Returns a copy of the dataframe
    """
    def merge(self, other, on=None):
        if on is None:
            on = self.columns & other.columns
        if len(on) == 0:
            return SimplDf()
        rows = []
        for row in self.rows:
            for orow in other.rows:
                if all([row[col] == orow[col] for col in on]):
                    row = {}
                    row.update(copy.deepcopy(row))
                    row.update(copy.deepcopy(orow))
                    rows.append(row)
                    orow['$#matched'] = True
                    row['$#matched'] = True
        rows += self._find_unmatched(self.rows, rows)
        rows += self._find_unmatched(other.rows, rows)
        for row in rows:
            if '$#matched' in row:
                del row['$#matched']
        return SmallDf(rows=rows)

    def _find_unmatched(self, orig_rows, new_rows):
        unmatched = []
        for row in orig_rows:
            if '$#matched' not in row:
                unmatched.append(row)
        return unmatched

    """
    Identify a subset of rows
    A subset of the dataset is returned
    Values of the original dataframe can be modified
    """
    def qloc(self, *idxer):
        func, cols = self._query_args(*idxer)
        rows = self.rows
        if func is not None:
            rows = [row for row in rows if func(row)]
        df = SmallDf(rows=rows, cols=cols, is_loc=True)
        return df

    """
    Identify a subset of rows
    A deepcopy of the dataset is returned
    Values of the original dataframe cannot be modified
    """
    def query(self, *idxer):
        func, cols = self._query_args(*idxer)
        rows = copy.deepcopy(self.rows)
        if func is not None:
            rows = [row for row in rows if func(row)]
        df = SmallDf(rows=rows, cols=cols)
        return df

    """
    Parse arguments for querying
    """
    def _query_args(self, *idxer):
        if len(idxer) == 1:
            idxer = idxer[0]
            if callable(idxer):
                return idxer, None
            elif isinstance(idxer, Iterable) or isinstance(idxer, str):
                return None, idxer
        if len(idxer) == 2:
            if callable(idxer[0]) and \
               (isinstance(idxer[1], Iterable) or isinstance(idxer[1], str)):
                return idxer[0], idxer[1]
        raise Exception("Invlaid parameters to query or loc")

    """
    Apply a function to all rows
    Modifies the dataframe in-place.
    """
    def apply(self, func):
        for row in self.rows:
            for col in self.columns:
                row[col] = func(row, col)
        return self

    """
    Fill None values
    """
    def fillna(self, val):
        self.apply(lambda r, c: val if r[c] is None else r[c])
        return self

    """
    A subset of columns from the two dfs
    """
    def __getitem__(self, idxer):
        if self.is_loc:
            if isinstance(idxer, tuple):
                return self.qloc(*idxer)
            else:
                return self.qloc(idxer)
        else:
            if isinstance(idxer, tuple):
                return self.query(*idxer)
            else:
                return self.query(idxer)

    """
    Assign 
    """
    def __setitem__(self, idxer, other):
        if isinstance(idxer, tuple):
            df = self.qloc(*idxer)
        else:
            df = self.qloc(idxer)
        if isinstance(other, SmallDf):
            if len(df.rows) != len(other.rows):
                raise Exception("Number of rows in dfs different")
            if len(df.columns) != len(other.columns):
                raise Exception("Column names don't match")
            for row, orow in zip(df.rows, other.rows):
                for col, ocol in zip(df.columns, other.columns):
                    row[col] = orow[ocol]
        else:
            for row in df.rows:
                for col in cols:
                    row[col] = other

    """
    Apply an arithmetic op
    """
    def _op(self, other, func):
        if isinstance(other, SmallDf):
            if len(self.rows) != len(other.rows):
                raise Exception("Number of rows in dfs different")
            if len(self.columns) != len(other.columns):
                raise Exception("Column names don't match")
            rows = [{col: func(row, col, orow, ocol)}
                    for row, orow in zip(self.rows, other.rows)
                    for col, ocol in zip(self.columns, other.columns)]
        else:
            rows = [{col: row[col] + other}
                    for row in self.rows
                    for col in self.columns]
        return SmallDf(rows=rows)

    """
    Add two dfs together
    """
    def __add__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] + orow[ocol])

    """
    Subtract two dfs
    """
    def __sub__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] - orow[ocol])

    """
    Multiply two dfs
    """
    def __mul__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] * orow[ocol])

    """
    Divide two dfs
    """
    def __truediv__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] / orow[ocol])

    """
    Length of this df (# rows)
    """
    def __len__(self):
        return len(self.rows)

    """
    Ensure that all rows have the same columns
    """
    def _correct_rows(self):
        for row in self.rows:
            self._correct_row(row)

    """
    Ensure that a particular row has all columns
    """
    def _correct_row(self, row):
        for col in self.columns:
            if col not in row:
                row[col] = None
        keys = list(row.keys())
        for col in keys:
            if col not in self.columns:
                del row[col]

    """
    Save to YAML
    """
    def to_yaml(self, path):
        YamlFile(path).save(self.rows)

    """
    Load from YAML
    """
    def load_yaml(self, path):
        self.rows = YamlFile(path).load()
