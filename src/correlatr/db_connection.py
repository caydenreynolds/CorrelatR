from base64 import b64encode, b64decode

import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .response import create_response
from protos import shared_pb2

#Create a sessionmaker singleton
_Session = sessionmaker()

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
        _Session.configure(bind=self._engine)
        self.table_name = table_name
        self._session = _Session()

        #This will create the new table if and only if it does not exist alreadu
        Base = declarative_base()
        class UserTable(Base):
            __tablename__ = table_name

            DATE = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
        Base.metadata.create_all(self._engine)


    def get_all_points_in_columns(self, column1, column2):
        """Gets all points from both of the given columns.
        Points are only returned if data is present in both columns for a given row.
        Data is returned as two lists, where all values from column1 are in the first list,
        and all values in column2 are in the second list. Additionally, list1[0] and list2[0] 
        are from the same row in the database

        Args:
            column1 (str): The first column to pull values from
            column2 (str): The second column to pull values from
        
        Returns:
            Two lists, with all values from column1 and column 2
        """
        column1 = get_safe_column_name(column1)
        column2 = get_safe_column_name(column2)
        table = self._get_table()

        list1 = []
        list2 = [] 

        for row in self._session.query(table).all():
            row = row._asdict()
            if row[column1] is not None and row[column2] is not None:
                list1.append(row[column1])
                list2.append(row[column2])

        return list1, list2

    def set_data(self, date, data_points):
        """Sets the given data point values to the row identified by the given date

        Args:
            date (int): The date to set the datapoints to
            datapoints (dict): (Column, value) pairs to store in the database

        Returns: The response message to send to the client 
        """
        table = self._get_table()
        data = self._session.query(table).filter_by(DATE=date).one_or_none()
        self._session.commit()

        #Make column names safe
        safe_points = {}
        for key, value in data_points.items():
            safe_points[get_safe_column_name(key)] = value
        
        if data is None:
            ins = table.insert().values(DATE = date, **safe_points)
            self._engine.connect().execute(ins)
            return create_response('Inserting a new row into the database', False)
        else:
            upd = table.update().where(table.c.DATE == date).values(**safe_points)
            self._engine.connect().execute(upd)

            return create_response('Updating a row in the database', False)

    def get_data(self, date):
        """Get all of the data associated with a specific date as a list of tuples

        Args:
            date (int): Date to get data from
        
        Returns: A list of tuples describing (column_name, data)
        """
        table = self._get_table()
        data = self._session.query(table).filter_by(DATE=date).one_or_none()
        self._session.commit()
        if data is None:
            response = create_response('No row present in database for this date', False)
            columns = self.get_all_columns()
            for column in columns:
                data_point = shared_pb2.DataPoint()
                data_point.columnName = column
                data_point.null = True
                response.dataPoints.append(data_point)
        else:
            data = data._asdict()
            response = create_response('Row already present in database for this date', False)
            for column in table.c:
                column_name = str(column).partition('.')[2]
                if column_name != "DATE":
                    data_point = shared_pb2.DataPoint()
                    data_point.columnName = get_actual_column_name(column_name)

                    if data[column_name]:
                        data_point.value = data[column_name]
                    else:
                        data_point.null = True
                    response.dataPoints.append(data_point)
                    
        return response

    def get_all_columns(self):
        """Get the names of all the columns in the table

        Returns: The list of column names
        """
        table = self._get_table()
        columns = []
        for column in table.c:
            if column.name != "DATE":
                columns.append(get_actual_column_name(column.name))

        return columns

    def add_column(self, column_name):
        """Adds a column to the table

        Args:
            column_name (str): Name of the new column

        Returns: The response message to send to the client
        """
        safe_column_name = get_safe_column_name(column_name)
        table = self._get_table()
        if not safe_column_name:
            return create_response(f"Cannot create column without a name!", True)
        elif safe_column_name in table.c:
            return create_response(f"{column_name} already exists", True)
        elif len(safe_column_name) > 63:
            return create_response(f"Column name {column_name} is too long!", True)
        else:
            self._engine.connect().execute(f'ALTER TABLE {self.table_name} ADD COLUMN "{safe_column_name}" float')
            return create_response(f"{column_name} has been added", False)

    def remove_column(self, column_name):
        """Removes a column from the database table

        Args:
            column_name (str): Name of the column to remove

        Returns: The response message to send to the client
        """
        safe_column_name = get_safe_column_name(column_name)
        table = self._get_table()
        if safe_column_name not in table.c:
            return create_response(f"{column_name} is not in the table", True)
        else:
            self._engine.connect().execute(f'ALTER TABLE {self.table_name} DROP COLUMN "{safe_column_name}"')
            return create_response(f"{column_name} has been removed", False)

    def rename_column(self, old_column_name, new_column_name):
        """Rename a column in the database table

        Args:
            new_column_name (str): The new name of the column
            old_column_name (str): The name of the column to change

        Returns: The response message to send to the client
        """
        safe_new_column_name = get_safe_column_name(new_column_name)
        safe_old_column_name = get_safe_column_name(old_column_name)
        table = self._get_table()

        if not new_column_name:
            return create_response(f'Cannot rename column to not have a name', True)
        if safe_old_column_name not in table.c:
            return create_response(f'{old_column_name} is not in the table', True)
        elif safe_new_column_name == safe_old_column_name:
            return create_response(f'{old_column_name} is the same as {new_column_name}', True)
        else:
            self._engine.connect().execute(f'ALTER TABLE {self.table_name} RENAME COLUMN "{safe_old_column_name}" TO "{safe_new_column_name}"')
            return create_response(f"{old_column_name} has been renamed to {new_column_name}", False)

    def _get_table(self):
        """Loads the 'table_name' database table

        Args:
            table_name (str): Name of the table to load

        Returns: SqlAlchemy Table reflected from 'table_name'
        """
        metadata = sqlalchemy.MetaData()
        return sqlalchemy.Table(self.table_name, metadata, autoload=True, autoload_with=self._engine)

def get_safe_column_name(column_name):
    """Get the safe sql column name from the given string

    Args:
        column_name (str): name of the column to make safe

    Returns: The safe name of the column
    """
    return b64encode(column_name.encode("utf-8")).decode("ascii")

def get_actual_column_name(safe_column_name):
    """Converts the safe sql column name to an actual column name

    Args:
        safe_column_name (str): The safe column name

    Returns: The actual name of the column
    """
    return b64decode(safe_column_name.encode("ascii")).decode("utf-8")


