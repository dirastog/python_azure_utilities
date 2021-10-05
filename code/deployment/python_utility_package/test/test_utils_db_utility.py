from unittest import TestCase
from unittest.mock import patch, Mock

import pandas as pd

from utility_package.utils.db_utility import DBConnection


server_name = 'test-ss.database.windows.net'
db_name = 'test-db'
user_name = 'username'
pwd = 'pwd'
tenant_id = 'tenant_id'
test_module_name = 'utility_package.utils.db_utility'


class TestDBConnection(TestCase):
    @patch(f'{test_module_name}.pyodbc')
    def test_init(self, mock_pyodbc):
        mock_pyodbc_connection = Mock()
        mock_pyodbc.connect.return_value = mock_pyodbc_connection
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )

        self.assertEqual(dbobj.connection, mock_pyodbc_connection)
        self.assertTrue(mock_pyodbc_connection.autocommit)

    @patch(f'{test_module_name}.adal')
    @patch(f'{test_module_name}.pyodbc')
    def test_init_spn_auth(self, mock_pyodbc, mock_adal):
        token = {
            'accessToken': 'abcdefgh'
        }
        mock_pyodbc_connection = Mock()
        mock_pyodbc.connect.return_value = mock_pyodbc_connection
        mock_adal_auth_context = Mock()
        mock_adal_auth_context.acquire_token_with_client_credentials.\
            return_value = token
        mock_adal.AuthenticationContext.return_value = mock_adal_auth_context
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd,
            spn_auth=True,
            tenant_id=tenant_id
        )
        self.assertEqual(dbobj.connection, mock_pyodbc_connection)

    @patch(f'{test_module_name}.pyodbc')
    def test_run_sql_query_1(self, mock_pyodbc):
        result_set = [
            ('ID1', 1),
            ('ID2', 2),
            ('ID3', 3),
            ('ID4', 4)
        ]
        columns_tuple = [('Col1', 1), ('Col2', 2)]
        mock_pyodbc_connection = Mock()
        mock_pyodbc.connect.return_value = mock_pyodbc_connection
        mock_pyodbc_cursor = Mock()
        mock_pyodbc_connection.cursor.return_value = mock_pyodbc_cursor
        mock_pyodbc_cursor.description = columns_tuple
        mock_pyodbc_cursor.fetchall.return_value = result_set

        column_names = [items[0] for items in columns_tuple]
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        query = 'SELECT * FROM dbo.Table1'
        results, columns = dbobj.run_sql_query(query)
        self.assertEqual(dbobj.connection.cursor(), mock_pyodbc_cursor)
        self.assertEqual(results, result_set)
        self.assertEqual(columns, column_names)

    @patch(f'{test_module_name}.pyodbc')
    def test_run_sql_query_2(self, mock_pyodbc):
        mock_pyodbc_connection = Mock()
        mock_pyodbc.connect.return_value = mock_pyodbc_connection
        mock_pyodbc_cursor = Mock()
        mock_pyodbc_connection.cursor.return_value = mock_pyodbc_cursor
        mock_pyodbc_cursor.description = None

        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        query = 'SELECT * FROM dbo.Table1'
        results, columns = dbobj.run_sql_query(query)
        self.assertEqual(dbobj.connection.cursor(), mock_pyodbc_cursor)
        self.assertIsNone(results)
        self.assertIsNone(results)

    @patch(f'{test_module_name}.pyodbc')
    def test_run_stored_proc_1(self, mock_pyodbc):
        result_set = [
            ('ID1', 1),
            ('ID2', 2),
            ('ID3', 3),
            ('ID4', 4)
        ]
        columns_tuple = [('Col1', 1), ('Col2', 2)]
        mock_pyodbc_connection = Mock()
        mock_pyodbc.connect.return_value = mock_pyodbc_connection
        mock_pyodbc_cursor = Mock()
        mock_pyodbc_connection.cursor.return_value = mock_pyodbc_cursor
        mock_pyodbc_cursor.description = columns_tuple
        mock_pyodbc_cursor.fetchall.return_value = result_set

        column_names = [items[0] for items in columns_tuple]
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        schema = 'dbo'
        stored_proc = 'demoSP'
        params = dict()
        params['param1'] = 'value1'
        params['param2'] = 'value2'

        stmt = f"SET NOCOUNT ON;EXEC [{schema}].[{stored_proc}]"
        stmt += f"@param1='{params['param1']}',"
        stmt += f"@param2='{params['param2']}';"

        results, columns = dbobj.run_stored_proc(
            schema, stored_proc,
            params=params
        )
        self.assertEqual(dbobj.connection.cursor(), mock_pyodbc_cursor)
        mock_pyodbc_cursor.execute.assert_called_once_with(stmt)
        self.assertEqual(results, result_set)
        self.assertEqual(columns, column_names)

    @patch(f'{test_module_name}.pyodbc')
    def test_run_stored_proc_2(self, mock_pyodbc):
        mock_pyodbc_connection = Mock()
        mock_pyodbc.connect.return_value = mock_pyodbc_connection
        mock_pyodbc_cursor = Mock()
        mock_pyodbc_connection.cursor.return_value = mock_pyodbc_cursor
        mock_pyodbc_cursor.description = None

        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        schema = 'dbo'
        stored_proc = 'demoSP'
        params = dict()
        params['param1'] = 'value1'
        params['param2'] = 'value2'

        stmt = f"SET NOCOUNT ON;EXEC [{schema}].[{stored_proc}]"
        stmt += f"@param1='{params['param1']}',"
        stmt += f"@param2='{params['param2']}';"

        results, columns = dbobj.run_stored_proc(
            schema, stored_proc,
            params=params
        )
        self.assertEqual(dbobj.connection.cursor(), mock_pyodbc_cursor)
        mock_pyodbc_cursor.execute.assert_called_once_with(stmt)
        self.assertIsNone(results)
        self.assertIsNone(columns)

    @patch(f'{test_module_name}.pyodbc')
    def test_get_df_from_result_set_1(self, mock_pyodbc):
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        result_df = dbobj.get_df_from_result_set()
        self.assertEqual(result_df.shape, (0, 0))

    @patch(f'{test_module_name}.pyodbc')
    def test_get_df_from_result_set_2(self, mock_pyodbc):
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        result_df = dbobj.get_df_from_result_set(
            results=[],
            columns=['col1', 'col2'])
        self.assertEqual(result_df.shape, (0, 2))

    @patch(f'{test_module_name}.pyodbc')
    def test_get_df_from_result_set_3(self, mock_pyodbc):
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        result_df = dbobj.get_df_from_result_set(
            results=[('ID1', 1), ('ID2', 2)],
            columns=['col1', 'col2']
            )
        self.assertEqual(result_df.shape, (2, 2))

    def test_format_result_set(self):
        results = [('ID1', 1), ('ID2', 2)]
        columns = ['col1', 'col2']
        data = DBConnection._format_result_set(results, columns)
        produced_results = [item for item in data]
        expected_results = [
                        {'col1': 'ID1', 'col2': 1},
                        {'col1': 'ID2', 'col2': 2}
                        ]
        self.assertEqual(produced_results, expected_results)

    def test_form_query_from_df_1(self):
        schema = 'dbo'
        sp_name = 'uspUpdateModelInputDataset'
        input_df = pd.DataFrame([
            {
                'SKU_NUMBER': '123456',
                'POS_SALES': '82.3',
                'POS_QTY': '1'
            },
            {
                'SKU_NUMBER': '1234567',
                'POS_SALES': '80',
                'POS_QTY': '2'
            },
        ])
        table_type_name = 'TableType_ModelInputDataset'
        req_query = "DECLARE @InputVar [dbo].[TableType_ModelInputDataset] "
        req_query += "INSERT INTO @InputVar "
        req_query += "([SKU_NUMBER],[POS_SALES],[POS_QTY]) "
        req_query += "VALUES('123456','82.3','1'),"
        req_query += "('1234567','80','2');"
        req_query += "EXEC [dbo].[uspUpdateModelInputDataset] @InputVar"

        result_query = DBConnection.form_query_from_df(
            schema,
            sp_name,
            input_df,
            table_type_name
        )
        self.assertEqual(req_query, result_query)

    def test_form_query_from_df_2(self):
        schema = 'dbo'
        sp_name = 'uspUpdateModelInputDataset'
        input_df = pd.DataFrame()
        table_type_name = 'TableType_ModelInputDataset'

        result_query = DBConnection.form_query_from_df(
            schema,
            sp_name,
            input_df,
            table_type_name
        )
        self.assertIsNone(result_query)

    @patch(f'{test_module_name}.pyodbc')
    def test_insert_data_from_df(self, mock_pyodbc):
        dbobj = DBConnection(
            server_name,
            db_name,
            user_name,
            pwd
        )
        mock_run_sql_query = Mock()
        mock_form_query_from_df = Mock()
        dbobj.run_sql_query = mock_run_sql_query
        dbobj.form_query_from_df = mock_form_query_from_df

        test_input_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        dbobj.insert_data_from_df(
            schema='test_schema',
            sp_name='test_sp_name',
            input_df=test_input_df,
            table_type_name='test_table_type_name'
        )

        mock_form_query_from_df.assert_called_once_with(
            'test_schema',
            'test_sp_name',
            test_input_df,
            'test_table_type_name'
        )
        mock_run_sql_query.assert_called_once()
