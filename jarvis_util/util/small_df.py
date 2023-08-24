"""
This module provides a simple database implementation which stored
and saved in a human-readable format.
"""
from jarvis_util.serialize.yaml_file import YamlFile
from jarvis_util.util.import_mod import load_class
from collections.abc import Iterable
import copy
import yaml


class SmallDf:
    """
    This class provides a simple database implementation which stored
    and saved in a human-readable format.

    :param rows: List[Dict] of entries
    :param columns: List or string of columns
    :param is_loc: Is this df being used for indexing? This will avoid
    destroying columns by accident
    """
    def __init__(self, rows=None, columns=None, is_loc=False):
        self.rows = []
        self.columns = set()
        self.is_loc = is_loc
        if rows is not None:
            if not is_loc:
                self.concat(rows)
            else:
                self.rows = rows
                self.infer_columns()
        if columns is not None:
            self.set_columns(columns)
        if not is_loc:
            self.loc = SmallDf(is_loc=True)
            self.loc.columns = self.columns
            self.loc.rows = self.rows

    """
    Concatenate a dataframe (or records) to this one
    """
    def concat(self, df):
        if len(df) == 0:
            return self
        if isinstance(df, SmallDf):
            self.rows += df.rows
            self.columns.update(df.columns)
        elif isinstance(df, list):
            rows = df
            if not isinstance(rows[0], dict):
                rows = {col: val for row in rows
                        for col, val in zip(self.columns, rows)}
            self.rows += rows
            self.infer_columns(df)
        self._correct_rows()
        return self

    """
    Remove duplicate entries
    """
    def drop_duplicates(self, inplace=True):
        dedup = self._drop_duplicates(self.rows)
        self.rows.clear()
        self.rows += dedup

    def _drop_duplicates(self, rows):
        dedup = list(set(self._fixed_dict(rows)))
        return self._mutable_dict(dedup)

    def _fixed_dict(self, rows):
        return tuple((tuple(row.items()) for row in rows))

    def _mutable_dict(self, rows):
        return [{key:val for key, val in row} for row in rows]

    """
    Set the columns
    """
    def set_columns(self, columns):
        if not isinstance(columns, (list, tuple, set)):
            columns = [columns]
        self.columns.clear()
        self.columns.update(columns)
        if not self.is_loc:
            self._correct_rows()
        return self

    """
    Infer columns based on the rows
    """
    def infer_columns(self, rows=None):
        if rows is None:
            rows = self.rows
        for row in rows:
            self.columns.update(row.keys())

    """
    Add columns to the table
    """
    def add_columns(self, columns):
        if columns is None:
            return self
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        if not any([col not in self.columns for col in columns]):
            return self
        self.columns.update(columns)
        self._correct_rows()
        return self

    """
    Remove columns from the table
    """
    def drop_columns(self, columns):
        if self.is_loc:
            raise Exception("Cannot remove columns from location df")
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        if len(columns) == 0:
            return
        self.columns.difference_update(columns)
        self._correct_rows()
        return self

    """
    Analagous to drop_columns
    """
    def drop(self, columns):
        return self.drop_columns(columns)

    """
    Rename a column
    
    :param columns: Dict[OldName:NewName]
    """
    def rename(self, columns):
        self.columns.difference_update(columns.keys())
        self.columns.update(columns.values())
        for row in self.rows:
            for old_name, new_name in columns.items():
                row[new_name] = row.pop(old_name)
        return self

    """
    Merge this dictionary with another
    Returns a copy of the dataframe
    """
    def merge(self, other, on=None):
        if on is None:
            on = self.columns & other.columns
        if len(on) == 0:
            return SmallDf()
        rows = []
        for row in self.rows:
            for orow in other.rows:
                if all([row[col] == orow[col] for col in on]):
                    merge_row = {}
                    merge_row.update(copy.deepcopy(row))
                    merge_row.update(copy.deepcopy(orow))
                    rows.append(merge_row)
                    orow['$#matched'] = True
                    row['$#matched'] = True
        rows += self._find_unmatched(self.rows, rows)
        rows += self._find_unmatched(other.rows, rows)
        for row in rows:
            if '$#matched' in row:
                del row['$#matched']
        columns = self.columns.union(other.columns)
        return SmallDf(rows=rows, columns=columns)

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
        func, columns = self._query_args(*idxer)
        rows = self.rows
        if func is not None:
            rows = [row for row in rows if func(row)]
        df = SmallDf(rows=rows, columns=columns, is_loc=True)
        self.add_columns(columns)
        return df

    """
    Identify a subset of rows
    A deepcopy of the dataset is returned
    Values of the original dataframe cannot be modified
    """
    def query(self, *idxer):
        func, columns = self._query_args(*idxer)
        rows = copy.deepcopy(self.rows)
        if func is not None:
            rows = [row for row in rows if func(row)]
        df = SmallDf(rows=rows, columns=columns)
        return df

    """
    Parse arguments for querying
    """
    def _query_args(self, *idxer):
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
                raise Exception("Invlaid parameters to query or loc")
            if callable(idxer[0]):
                func = idxer[0]
            elif isinstance(idxer[0], slice):
                func = None
            else:
                raise Exception("Invlaid parameters to query or loc")
            return func, columns
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
    def fillna(self, val, inplace=True):
        self.apply(lambda r, c: val if r[c] is None else r[c])
        return self

    """
    Get unique values
    """
    def unique(self):
        df = self.copy()
        df.drop_duplicates()
        return df

    """
    List operator
    """
    def list(self):
        return [list(row.values()) for row in self.rows]

    """
    Sort
    """
    def sort_values(self, col):
        self.rows.sort(key=lambda x: x[col])
        return self

    """
    Group by a combo of columns
    """
    def groupby(self, columns):
        smallgrpby = load_class('jarvis_util.util.small_df', '', 'SmallGroupBy')
        return SmallGroupBy(columns, self.rows)

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
                for col in df.columns:
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
    Apply an arithmetic op
    """
    def _opeq(self, other, func):
        df = self._op(other, func)
        for row, orow in zip(self.rows, df.rows):
            for col in df.columns:
                row[col] = orow[col]
        return self

    """
    Add two dfs together
    """
    def __add__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] + orow[ocol])
    def __iadd__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    """
    Subtract two dfs
    """
    def __sub__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] - orow[ocol])
    def __isub__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    """
    Multiply two dfs
    """
    def __mul__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] * orow[ocol])
    def __imul__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

    """
    Divide two dfs
    """
    def __truediv__(self, other):
        return self._op(other,
                        lambda row, col, orow, ocol: row[col] / orow[ocol])
    def __itruediv__(self, other):
        return self._opeq(other,
                          lambda row, col, orow, ocol: row[col] + orow[ocol])

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

    """
    Convert into a nice string
    """
    def to_string(self):
        return yaml.dump(self.rows)
    def __str__(self):
        return self.to_string()
    def __repr__(self):
        return self.to_string()

    """
    Copy
    """
    def copy(self):
        df = SmallDf(rows=self.rows, columns=self.columns)
        return df


""" Concat a list of dfs """
def concat(dfs):
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

""" Merge two dfs """
def merge(dfs, on=None, how=None):
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

"""
GroupBy object
"""
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
            key = tuple([row[col] for col in self.columns])
            if key not in self.groups:
                self.groups[key] = []
            self.groups[key].append(row)
        for key, rows in self.groups.items():
            self.groups[key] = SmallDf(rows=self.groups[key])

    """
    Expand the groupby into a SmallDf
    """
    def reset_index(self, *args, **kwargs):
        rows = []
        for grp_df in self.groups.values():
            rows += grp_df.rows
        return SmallDf(rows=rows)

    """
    Keep only elements meeting the condition
    """
    def filter(self, func):
        grp = SmallGroupBy()
        for key, grp_df in self.groups.items():
            grp.groups[key] = SmallDf(
                rows=[row for row in grp_df.rows if func(row)])
        return grp

    """
    Keep only groups meeting the condition
    """
    def filter_groups(self, func):
        grp = SmallGroupBy()
        for key, grp_df in self.groups.items():
            if func(grp_df):
                grp.groups[key] = grp_df
        return grp

    """
    Get the first element in each group
    """
    def first(self):
        return self.head(1)

    """
    Get the first "n" elements in each group
    """
    def head(self, n):
        grp = SmallGroupBy()
        for key, grp_df in self.groups.items():
            grp.groups[key] = SmallDf(rows=grp_df.rows[0:n])
        return grp

    """
    Get the minimum per-group
    """
    def min(self):
        pass

    """
    Get the maixmum per-group
    """
    def max(self):
        pass

    """
    Get the number of groups
    """
    def __len__(self):
        return len(self.groups)
