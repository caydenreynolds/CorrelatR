import subprocess
from unittest import TestCase, mock

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

from correlatr import db_connection

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

    def tearDown(self):
        for column in self.db_conn._get_table().c:
            column = str(column).rpartition('.')[2]
            if column != 'DATE':
                self.db_conn._engine.execute(f'ALTER TABLE {self.table_name} DROP COLUMN "{column}"')

    def test_get_safe_column_name(self):
        actual = self.db_conn._get_safe_column_name(self.column_name)
        self.assertEqual(actual, self.safe_column_name)

    def test_get_actual_column_name(self):
        actual = self.db_conn._get_actual_column_name(self.safe_column_name)
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
        self.assertTrue(self.db_conn._get_safe_column_name('bar') in self.db_conn._get_table().c)
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
        self.assertTrue(self.db_conn._get_safe_column_name(self.column_name) in self.db_conn._get_table().c)
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
        self.assertTrue(self.db_conn._get_safe_column_name(self.column_name) in self.db_conn._get_table().c)
        self.assertEqual(len(self.db_conn._get_table().c), 2)

    def test_get_all_columns(self):
        columns = ['foo', 'bar', 'hello world']
        for column in columns:
            self.db_conn.add_column(column)

        for column in self.db_conn.get_all_columns():
            self.assertTrue(column in columns)

        self.assertTrue(len(self.db_conn.get_all_columns()), columns)


