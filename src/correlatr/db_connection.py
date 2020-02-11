from base64 import b64encode, b64decode

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

from .response import create_response

class DBConnection:
    TABLE_NAME = 'user_data'

    def __init__(self, url, table_name):
        """A class that handles operations that interact with
        the database

        Args:
            url (str): The database url to connect to
            table_name (str): The name of the table to operate on
        """
        self._engine = sqlalchemy.create_engine(url)
        self.table_name = table_name

        Base = declarative_base()
        class UserTable(Base):
            __tablename__ = table_name

            DATE = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

        #This will create the new table if and only if it does not exist alreadu
        Base.metadata.create_all(self._engine)

    def get_all_columns(self):
        """Get the names of all the columns in the table

        Returns: The list of column names
        """
        table = self._get_table()
        columns = []
        for column in table.c:
            if column.name != "DATE":
                columns.append(self._get_actual_column_name(column.name))

        return columns

    def add_column(self, column_name):
        """Adds a column to the table

        Args:
            column_name (str): Name of the new column

        Returns: The response message to send to the client
        """
        safe_column_name = self._get_safe_column_name(column_name)
        table = self._get_table()
        if not safe_column_name:
            return create_response(f"Cannot create column without a name!", True)
        elif safe_column_name in table.c:
            return create_response(f"{column_name} already exists", True)
        else:
            self._engine.execute(f'ALTER TABLE {self.table_name} ADD COLUMN "{safe_column_name}" float')
            return create_response(f"{column_name} has been added", False)

    def remove_column(self, column_name):
        """Removes a column from the database table

        Args:
            column_name (str): Name of the column to remove

        Returns: The response message to send to the client
        """
        safe_column_name = self._get_safe_column_name(column_name)
        table = self._get_table()
        if safe_column_name not in table.c:
            return create_response(f"{column_name} is not in the table", True)
        else:
            self._engine.execute(f'ALTER TABLE {self.table_name} DROP COLUMN "{safe_column_name}"')
            return create_response(f"{column_name} has been removed", False)

    def rename_column(self, old_column_name, new_column_name):
        """Rename a column in the database table

        Args:
            new_column_name (str): The new name of the column
            old_column_name (str): The name of the column to change

        Returns: The response message to send to the client
        """
        safe_new_column_name = self._get_safe_column_name(new_column_name)
        safe_old_column_name = self._get_safe_column_name(old_column_name)
        table = self._get_table()

        if not new_column_name:
            return create_response(f'Cannot rename column to not have a name', True)
        if safe_old_column_name not in table.c:
            return create_response(f'{old_column_name} is not in the table', True)
        elif safe_new_column_name == safe_old_column_name:
            return create_response(f'{old_column_name} is the same as {new_column_name}', True)
        else:
            self._engine.execute(f'ALTER TABLE {self.table_name} RENAME COLUMN "{safe_old_column_name}" TO "{safe_new_column_name}"')
            return create_response(f"{old_column_name} has been renamed to {new_column_name}", False)

    def _get_table(self):
        """Loads the 'table_name' database table

        Args:
            table_name (str): Name of the table to load

        Returns: SqlAlchemy Table reflected from 'table_name'
        """
        metadata = sqlalchemy.MetaData()
        return sqlalchemy.Table(self.table_name, metadata, autoload=True, autoload_with=self._engine)

    def _get_safe_column_name(self, column_name):
        """Get the safe sql column name from the given string

        Args:
            column_name (str): name of the column to make safe

        Returns: The safe name of the column
        """
        return b64encode(column_name.encode("utf-8")).decode("ascii")

    def _get_actual_column_name(self, safe_column_name):
        """Converts the safe sql column name to an actual column name

        Args:
            safe_column_name (str): The safe column name

        Returns: The actual name of the column
        """
        return b64decode(safe_column_name.encode("ascii")).decode("utf-8")


