import decimal

import pandas as pd
import pyodbc


class SQLException(Exception):
    pass


class DBConnection:
    '''
    Class DBConnection creates connection as a class attribute
    after the connection is established to the Server.
    spn_auth = False will do a sql server auth using username and pwd
    spn_auth = True will do a SPN auth using SPN client_id and client_secret
    Pass Client ID as user_name and Client Secret as pwd along with Tenant ID
    when spn_auth = True
    '''
    def __init__(self, server, db_name, user_name, pwd, **kwargs):
        self.server = server
        self.db_name = db_name
        self.user_name = user_name
        self.pwd = pwd
        self.driver = kwargs.get('driver', '{ODBC Driver 17 for SQL Server}')
        self.connection = None
        self.args = kwargs
        self.refresh_connection()

    def refresh_connection(self):
        spn_auth = self.args.get('spn_auth', False)
        if spn_auth is False:
            self.sql_server_auth()
        else:
            tenant_id = self.args.get('tenant_id')
            authority_host_url = self.args.get(
                'authority_host_url', 'https://login.microsoftonline.com')
            resource_uri = self.args.get(
                'resource_uri', "https://database.windows.net/")
            self.spn_sql_auth(
                authority_host_url,
                resource_uri,
                tenant_id)

    def sql_server_auth(self):
        '''
        This method gets invoked when the spn_auth flag is False
        and does the basic auth using username and pwd
        '''
        try:
            connstring = f'DRIVER={self.driver};'
            connstring += f'SERVER={self.server};'
            connstring += f'DATABASE={self.db_name};'
            connstring += f'UID={self.user_name};'
            connstring += f'PWD={self.pwd}'

            self.connection = pyodbc.connect(connstring)
            self.connection.autocommit = True

        except Exception as err:
            if self.connection is not None:
                self.connection.close()
            raise SQLException(err)

    def run_sql_query(self, query):
        '''
        This function runs a query and return the column names and the
        result set
        if both results and columns are None, then the query was a n
        on-result set query.
        if results is an empty list and column names are not None,
        the query returned an empty result set
        if both results and columns is a non empty list, then the query is a
        non empty result set query.
        '''
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
        # Connection timeout
        except pyodbc.OperationalError:
            self.refresh_connection()
            cursor = self.connection.cursor()
            cursor.execute(query)
        # check if cursor is result set cursor or non-result set cursor
        if cursor.description is not None:
            results = cursor.fetchall()
            columns = [items[0] for items in cursor.description]
        else:
            results = None
            columns = None
        cursor.close()
        return results, columns

    def run_stored_proc(self, schema, stored_proc, params=None):
        '''
        This function accepts teh schema name, stored proc name and
        parmas(dict).
        The function runs the SP with the desired params and then returns
        result set and columns if any.
        '''
        stored_proc_name = f'[{schema}].[{stored_proc}]'
        sql_stmt = f"SET NOCOUNT ON;EXEC {stored_proc_name}"
        if params is not None:
            for key, value in params.items():
                if isinstance(value, str):
                    sql_stmt += f"@{key}='{value}',"
                else:
                    sql_stmt += f"@{key}={value},"
            sql_stmt = sql_stmt.rstrip(',')
        sql_stmt += ';'
        cursor = self.connection.cursor()
        cursor.execute(sql_stmt.replace('None', 'NULL'))
        if cursor.description is not None:
            results = cursor.fetchall()
            columns = [cols[0] for cols in cursor.description]
        else:
            results = None
            columns = None
        cursor.close()
        return results, columns

    def get_df_from_result_set(self, results=None, columns=None):
        '''
        This function returns the df using the result set and column
        names that are passed as arguments.
        Returns an empty data frame if both result set and
        columns are none
        Returns a zero row dataframe with only column names if the query is
        an empty query
        Returns a datframe with the result set if result set has rows and
        column name list is not none
        '''
        if results is None and columns is None:
            result_df = pd.DataFrame()
        elif len(results) == 0:
            result_df = pd.DataFrame(columns=columns, dtype=object)
        else:
            data = self._format_result_set(results, columns)
            result_df = pd.DataFrame(data)
        return result_df

    def get_df_from_stored_proc(self, schema, stored_proc, params=None):
        results, columns = self.run_stored_proc(schema, stored_proc, params)
        return self.get_df_from_result_set(results=results, columns=columns)

    def get_df_from_query(self, query):
        results, columns = self.run_sql_query(query)
        return self.get_df_from_result_set(results=results, columns=columns)

    @staticmethod
    def _format_result_set(results, columns):
        '''
        This function acts as a record generator.
        '''
        for result in results:
            output = {}
            for i, key in enumerate(columns):
                if isinstance(result[i], decimal.Decimal):
                    output[key] = float(result[i])
                else:
                    output[key] = result[i]
            yield output

    @staticmethod
    def form_query_from_df(schema, sp_name, input_df, table_type_name):
        '''
        This is a static method which can be used to create dynamic
        sp execution query with one table type parameter from a df as
        an input
        '''
        query_start = f"DECLARE @InputVar [{schema}].[{table_type_name}] "
        query_start += "INSERT INTO @InputVar ("
        if len(input_df) == 0:
            return None
        for col in input_df.columns:
            query_start += f"[{col}],"
        query_start = query_start.rstrip(",")
        query_start += ") "
        query_rows = ""
        for _, row in input_df.iterrows():
            each_row = "SELECT "
            for item in row:
                if isinstance(item, (pd.Timestamp, str)):
                    each_row += f"'{item}',"
                else:
                    if pd.isnull(item):
                        each_row += "NULL,"
                    else:
                        each_row += f"{item},"
            each_row = each_row.rstrip(",")
            each_row += " UNION ALL "
            query_rows += each_row
        query_rows = query_rows.rstrip(" UNION ALL ")
        query_rows += ';'
        query_end = f"EXEC [{schema}].[{sp_name}] @InputVar"
        query = query_start + query_rows + query_end
        return query

    def insert_data_from_df(self, schema, sp_name, input_df, table_type_name):
        query = self.form_query_from_df(
            schema,
            sp_name,
            input_df,
            table_type_name
        )
        self.run_sql_query(query)

    def get_result_df_from_input_df(
        self, schema, sp_name, input_df, table_type_name
    ):
        query = self.form_query_from_df(
            schema,
            sp_name,
            input_df,
            table_type_name
        )
        query = "SET NOCOUNT ON;" + query
        return self.get_df_from_query(query)
