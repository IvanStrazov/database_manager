# utf-8
# Python 3.7
# 2021-03-01


from database_manager import base
from database_manager.utils import columns2query


class Table(base.ClassicManager):
    """
    Class for managing tables.
    """
    
    def is_table(self, table):
        """
        !!!IN DEV!!!

        Parameters:
            table (str) - name of table.

        Returns:
            result (bool) - result of test.
        """

        pass
    
    def create_table(self, table, columns, dtypes):
        """
        Create new table.
        
        Parameters:
            table (str) - name of table.
            columns (list[str]) - list of columns.
            dtypes (list[str]) - list of column's data types.
        
        Example:
            create_table(
                'POT.TEST_TABLE',
                ['inn', 'org_name', 'balance'],
                ['varchar2(12)', 'varchar2(4000)', 'number(13, 2)']
            )
        """
        
        cols_types = zip(columns, dtypes)
        cols_types = map(lambda x: '"' + x[0] + '" ' + x[1], cols_types)
        cols_types = ", ".join(cols_types)
        
        super().inner_action(f"CREATE TABLE {table} ( {cols_types} )")

    def drop_table(self, table):
        """
        Drop table.
        
        Parameters:
            table (str) - name of table.
        """
        
        super().inner_action(f"DROP TABLE {table}")
    
    def truncate_table(self, table):
        """
        Truncate table.
        
        Parameters:
            table (str) - name of table.
        """
        
        super().inner_action(f"TRUNCATE TABLE {table}")
    
    def delete_from_table(self, table, where):
        """

        Parameters:
            table (str) - name of table.
            where (list[str]) - where's query condition.

        Example:
            >> table.delete_from_table('POT.TEST_TABLE', ['IS_ACTIVE=0', 'YEAR=2021'])
        """

        where_ = "AND ".join(where) if where else ""

        super().inner_action(f"DELETE FROM {table} WHERE 1=1 {where_}")


class Column(base.ClassicManager):
    """
    Class for managing columns.
    """

    def add_column(self, table, column, dtype):
        """
        Add new column to table.
        
        Parameters:
            table (str) - name of table.
            column (str) - name of column.
            dtype (str) - data type of column.
        """
        
        super().inner_action(f'ALTER TABLE {table} ADD "{column}" {dtype}')
    
    def add_columns(self, table, columns, dtypes):
        """
        Add new column to table.

        Parameters:
            table (str) - name of table.
            columns (str) - list of column's names.
            dtypes (str) - list of column's data types.
        """
        for column, dtype in zip(columns, dtypes):
            self.add_column(table, column, dtype)
    
    def drop_column(self, table, column):
        """
        Drop column from table.
        
        Parameters:
            table (str) - name of table.
            column (str) - name of column.
        """
        
        super().inner_action(f'ALTER TABLE {table} DROP COLUMN "{column}"')

    def drop_columns(self, table, columns):
        """
        Drop columns from table.

        Parameters:
            table (str) - name of table.
            columns (str) - list of column's names.
        """

        for column in columns:
            self.drop_column(table, column)
    

class Constraint(base.ClassicManager):
    """
    Class for managing constraints.
    """
    
    def _add_primary_key(self, table, constraint, **kwargs):
        """
        Add Primary Key constraint.

        Parameters:
            table (str) - name of table
            constraint (str) - name of constraint.
            **kwargs:
                columns (list) - list of columns for key in 'primary key' and 'unique'.
        """
        
        columns = columns2query(kwargs['columns'])
        super().inner_action(f"ALTER TABLE {table} ADD CONSTRAINT {constraint} PRIMARY KEY ( {columns} )")

    def _add_unique(self, table, constraint, **kwargs):
        """
        Add Unique constraint.

        Parameters:
            table (str) - name of table
            constraint (str) - name of constraint.
            **kwargs:
                columns (list) - list of columns for key in 'primary key' and 'unique'.
        """
        
        columns = columns2query(kwargs['columns'])
        super().inner_action(f"ALTER TABLE {table} ADD CONSTRAINT {constraint} UNIQUE ({columns})")

    def _add_check(self, table, constraint, **kwargs):
        """
        Add Check constraint.

        Parameters:
            table (str) - name of table
            constraint (str) - name of constraint.
            **kwargs:
                ch_cond (str) - condition for check.
        """
        
        ch_cond = kwargs["ch_cond"]
        super().inner_action(f"ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK ({ch_cond})")

    def add_constraint(self, table, constraint, ctype, **kwargs):
        """
        Add constraint on table.
        
        Parameters:
            table (str) - name of table
            constraint (str) - name of constraint.
            ctype (str) - type of constraint ('primary key', 'unique', 'check')
            **kwargs:
                columns (list) - list of columns for key in 'primary key' and 'unique'.
                ch_cond (str) - check condition.
        """
        
        _add_dict = {
            "primary key": self._add_primary_key,
            "unique": self._add_unique,
            "check": self._add_check,
        }
        
        if ctype in _add_dict:
            _add_dict.get(ctype)(table, constraint, **kwargs)
        else:
            raise ValueError(f"wrong value 'ctype': {ctype}")

    def drop_constraint(self, table, constraint):
        """
        Drop constraint from table.

        Parameters:
            table (str) - name of table
            constraint (str) - name of constraint.
        """
        
        super().inner_action(f"ALTER TABLE {table} DROP CONSTRAIN {constraint}")


class Index(base.ClassicManager):
    """
    Class for managing indices.
    """
    
    def create_index(self, table, index, columns):
        """
        Create index on table.
        
        Parameters:
            table (str) - name of table.
            index (str) - name for new index.
            columns (list) - list of columns for indexing.
        """
        
        self.inner_action(f"CREATE INDEX {index} ON {table} ({columns2query(columns)})")


class TableManager(Table, Column, Constraint, Index):
    pass


