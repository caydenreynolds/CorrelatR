import subprocess
from unittest import TestCase, mock

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

from correlatr import db_connection
from protos import shared_pb2

class TestDbConnection(TestCase):

    @classmethod
    def setUpClass(cls):
        subprocess.run('sudo service postgresql start', shell=True)

    @classmethod
    def tearDownClass(cls):
        subprocess.run('sudo service postgresql stop', shell=True)

    def setUp(self):
        database_url = 'postgresql://correlatr_server:correlatr_password@localhost/correlatr'
        self.table_name = 'test_table'
        self.db_conn = db_connection.DBConnection(database_url, self.table_name)
        self.column_name = 'foo'
        self.safe_column_name = 'Zm9v'
        self.columns = ['foo', 'bar', 'hello world']
        self.data = [5, 0.0, 6.08]
        self.data_points = {}

        for i, column in enumerate(self.columns):
            self.data_points[column] = self.data[i]

    def tearDown(self):
        #Remove table -- We have to drop the table to avoid hitting the column limit
        #Simply removing columns will not work
        # https://nerderati.com/2017/01/03/postgresql-tables-can-have-at-most-1600-columns/
        self.db_conn._engine.execute(f'DROP TABLE {self.table_name};')

    def test_get_safe_column_name(self):
        actual = db_connection.get_safe_column_name(self.column_name)
        self.assertEqual(actual, self.safe_column_name)

    def test_get_actual_column_name(self):
        actual = db_connection.get_actual_column_name(self.safe_column_name)
        self.assertEqual(actual, self.column_name)

    def test_add_column(self):
        result = self.db_conn.add_column(self.column_name)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertFalse(result.statusMessage.error)
        self.assertTrue(self.safe_column_name in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 2)

    def test_add_column_already_exists(self):
        result = self.db_conn.add_column(self.column_name)
        result = self.db_conn.add_column(self.column_name)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertTrue(result.statusMessage.error)
        self.assertTrue(self.safe_column_name in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 2)

    def test_add_column_empty_name(self):
        result = self.db_conn.add_column('')
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertTrue(result.statusMessage.error)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_add_column_LONG_name(self):
        result = self.db_conn.add_column('a' * 100)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertTrue(result.statusMessage.error)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_remove_column(self):
        self.db_conn.add_column(self.column_name)
        result = self.db_conn.remove_column(self.column_name)
        self.assertFalse(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_remove_column_does_not_exist(self):
        result = self.db_conn.remove_column(self.column_name)
        self.assertTrue(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_remove_column_empty_name(self):
        result = self.db_conn.remove_column('')
        self.assertTrue(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_rename_column(self):
        self.db_conn.add_column(self.column_name)
        result = self.db_conn.rename_column(self.column_name, 'bar')
        self.assertFalse(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertTrue(db_connection.get_safe_column_name('bar') in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 2)

    def test_rename_column_does_not_exist(self):
        result = self.db_conn.rename_column(self.column_name, 'bar')
        self.assertTrue(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_rename_column_same_name(self):
        self.db_conn.add_column(self.column_name)
        result = self.db_conn.rename_column(self.column_name, self.column_name)
        self.assertTrue(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertTrue(db_connection.get_safe_column_name(self.column_name) in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 2)

    def test_rename_column_old_empty(self):
        self.db_conn.add_column('')
        result = self.db_conn.rename_column('', 'bar')
        self.assertTrue(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 1)

    def test_rename_column_new_empty(self):
        self.db_conn.add_column(self.column_name)
        result = self.db_conn.rename_column(self.column_name, '')
        self.assertTrue(result.statusMessage.error)
        self.assertTrue('DATE' in self.db_conn._get_table().c)
        self.assertTrue(db_connection.get_safe_column_name(self.column_name) in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 2)

    def test_get_all_columns(self):
        for column in self.columns:
            self.db_conn.add_column(column)

        for column in self.db_conn.get_all_columns():
            self.assertTrue(column in self.columns)

        self.assertTrue(len(self.db_conn.get_all_columns()), self.columns)

    def test_set_data_none_exists(self):
        for column in self.columns:
            self.db_conn.add_column(column)

        response = self.db_conn.set_data(1, self.data_points)
        self.assertFalse(response.statusMessage.error)

    def test_set_data_some_exists(self):
        for column in self.columns:
            self.db_conn.add_column(column)

        self.db_conn.set_data(1, self.data_points)
        response = self.db_conn.set_data(1, {self.columns[0]: 3.14, self.columns[1]: None})
        self.assertFalse(response.statusMessage.error)

    def test_get_data_none_exists(self):
        for column in self.columns:
            self.db_conn.add_column(column)

        response = self.db_conn.get_data_for_date(1)
        self.assertEqual(len(response.dataPoints), 3)
        for point in response.dataPoints:
            self.assertTrue(point.columnName in self.columns)

    def test_get_data_some_exists(self):
        for column in self.columns:
            self.db_conn.add_column(column)

        self.db_conn.set_data(1, self.data_points)
        response = self.db_conn.get_data_for_date(1)
        self.assertEqual(len(response.dataPoints), 3)
        for point in response.dataPoints:
            self.assertTrue(point.columnName in self.columns)
            self.assertAlmostEqual(point.value, self.data_points[point.columnName], 5)

    def test_get_data_in_columns(self):
        for column in self.columns:
            self.db_conn.add_column(column)

        self.db_conn.set_data(1, self.data_points)
        self.db_conn.set_data(2, {'foo': 7, 'bar': 12})
        self.db_conn.set_data(3, {'foo': 3})

        result = self.db_conn.get_data_in_columns('foo', 'bar')
        self.assertListEqual(result, [(5.0, 0.0), (7.0, 12.0)])
