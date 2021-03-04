# utf-8
# Python 3.7
# 2021-03-01


from database_manager import table
from database_manager import loader


class DataBaseManager(table.TableManager, loader.Loader):
    pass
