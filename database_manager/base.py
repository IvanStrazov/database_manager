# utf-8
# Python 3.7
# 2020-03-01


from abc import ABC, abstractmethod

from database_manager import connect


class BaseManager(ABC):
    """
    Base abstract class for dataloaders.
    """

    def __init__(self, db):
        """
        Initialization.
        
        Parameters:
            db (dict) - database name and config.
        """

        self._db = db
        self._connection = connect.Connect

    @abstractmethod
    def test_connection(self): pass

    @abstractmethod
    def inner_action(self, request): pass


class ClassicManager(BaseManager):
    """
    """

    def test_connection(self):
        """
        Test of connection.
        """

        with self._connection(**self._db) as conn:
            _ = conn.cursor()
            conn.commit()
        print("Connection is available!")

    def inner_action(self, request):
        """
        Some actions into Oracle without outer data.
        
        Parameters:
            request (str) - SQL query.
        """

        request = request.upper()

        with self._connection(**self._db) as conn:
            cur = conn.cursor()
            cur.execute(request)
            conn.commit()
