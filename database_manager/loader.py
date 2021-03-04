# utf-8
# Python 3.7
# 2021-03-01


import pandas as pd
import tqdm

from database_manager import base
from database_manager import type_translator as tt


class Selector(base.ClassicManager):
    """
    Load some data from database.
    """

    def select(self, request, pd_mode=False, **kwargs):
        """
        Load data from Oracle.
        
        Parameters:
            request (str) - SQL query.
            pd_mode (bool) - indicator to use pd.DataFrame for selected data.
            **kwargs:
                columns (list) - column names.
                dtype (dict) - column types.

        Returns:
            data (dataframe if pd_mode else list of tuples)
        """

        request = request.upper()

        with self._connection(**self._db) as conn:
            if pd_mode:
                data = pd.read_sql_query(request, con=conn)
            else:
                cur = conn.cursor()
                cur.execute(request)
                data = cur.fetchall()
            conn.commit()

        # Column names
        if kwargs.get("columns", False):
            data.columns = kwargs["columns"]
        # Column data types
        if kwargs.get("dtype", False):
            for col, dtype in kwargs["dtype"].items():
                data.loc[:, col] = data.loc[:, col].astype(dtype)

        return data


class Inserter(base.ClassicManager):
    """
    Upload some data to database.
    """

    @staticmethod
    def __standard_nan(data):
        """
        Transform NULL's in data to standard form.

        Parameters:
            data (pd.DataFrame)

        Returns:
            data (list[tuple])
        """

        __nan_rules = {"None": None, "nan": None, "none": None, "inf": None}

        return list(map(tuple, data.astype(str).replace(__nan_rules).values))

    def __numpy_insert(self, request, data, step=None):
        """
        Insert numpy's array.

        Parameters:
            request (str) - SQL query.
            data (np.array, ndim=2) - data to insert.
            step (int) - size of distinct load.
        """

        with self._connection(**self._db) as conn:
            cur = conn.cursor()
            if step:
                for ind in tqdm.tqdm(range(0, data.shape[0], step)):
                    data2sql = data.iloc[ind: ind + step]
                    self.__numpy_insert(request, data2sql)
            else:
                data2sql = list(map(tuple, data))
                cur.executemany(request, data2sql)

            conn.commit()

    def __pandas_insert(self, request, data, step=None, cut_nan=False):
        """
        Insert pandas's dataframe.

        Parameters:
            request (str) - SQL query.
            data (pd.DataFrame) - data to insert.
            step (int) - size of distinct load.
            cut_nan (bool) - flag to replace different nans to None.
        """

        if step:
            for ind in tqdm.tqdm(range(0, data.shape[0], step)):
                data2sql = data.iloc[ind: ind + step]
                data2sql = self.__cut_nan(data) if cut_nan else data2sql.values
                self.__numpy_insert(request, data2sql)
        else:
            data2sql = self.__standard_nan(data) if cut_nan else data.values
            self.__numpy_insert(request, data2sql)

    def insert(self, request, data, step=None, cut_nan=False):
        """
        Insert data to Oracle.
        
        Parameters:
            request (str) - SQL query.
            data ([pd.DataFrame|np.array], ndim=2) - data to insert.
            step (int) - size of distinct load.
            cut_nan (bool) - flag to replace different nans to None.
        """

        request = request.upper()

        if isinstance(data, pd.DataFrame):
            self.__pandas_insert(request, data, step, cut_nan)
        elif isinstance(data, np.array):
            self.__numpy_insert(request, data, step, cut_nan)
        else:
            raise ValueError(f"wrong data type: {type(data)}")

    def create_and_insert(self, table, data, step=False, cut_nan=False):
        """
        Create in base new table and insert data.
        
        Parameters:
            table (str) - name of table for creation.
            data (pd.DataFrame, ndim=2) - data to insert.
            step (int) - size of distinct load.
            cut_nan (bool) - flag to replace different nans to None.
        """

        if not isinstance(data, pd.DataFrame):
            return ValueError(f"wrong data type: {type(data)}")

        columns = data.columns
        python_dtypes = data.dtypes
        oracle_dtypes = [tt.python2oracle.get(dtype, "VARCHAR2(1024)") for dtype in python_dtypes]

        cols_types = zip(columns, oracle_dtypes)
        cols_types = map(lambda x: '"' + x[0] + '" ' + x[1], cols_types)
        cols_types = ", ".join(cols_types)

        super().inner_action(f"CREATE TABLE {table} ( {cols_types} )")

        cols_string = ":" + ", :".join(columns)
        request = f"INSERT INTO {table} VALUES( {cols_string} )"
        self.insert(request, data, step, cut_nan)


class Loader(Selector, Inserter):
    pass
