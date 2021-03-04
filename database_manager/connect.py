# utf-8
# Python 3.7
# 2021-02-19


class Connect:
    """
    Universal connection to SQL database over context manager.
    """

    def __init__(self, db_name, db_config):
        """
        Initialization.
        
        Parameters:
            db_name (str) - database name (oracle/teradata/ms_server/sqlite).
            db_config (dict) - database connection configuration.
        """

        self.__db_name = db_name
        self.__db_config = db_config

        self.__conn_library = __import__({
                                             "oracle": "cx_Oracle",
                                             "teradata": "teradatasql",
                                             "ms_server": "pymssql",
                                             "sqlite": "sqlite3"
                                         }.get(self.__db_name, None))

    def __enter__(self):
        """
        Create connection.
        """

        self.__connection = self.__conn_library.connect(**self.__db_config)

        return self.__connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close connection.
        """

        self.__connection.close()
