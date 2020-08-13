import sqlite3
from sqlite3 import Error

class LocalDBMethods():

    def __init__(self, dir_db):

        self.conn = self.create_connection(dir_db)

    def commit_query(self):
        self.conn.commit()

    # DB File 을 만들어 주는 혹은 연결 해 주는 함수
    def create_connection(self, db_file):
        # Create a database connection to a sQLITE database
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)
            print("DB connection is invalid")
        return conn

    # DB 내에 Table을 만들어 주는 함수(commit 을 하지 않기 때문에 insert나 update는 안됨)
    def excecute_sql_query(self, sql_query):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = self.conn.cursor()
            c.execute(sql_query)
            c.close()
        except Error as e:
            print(e)

    # DB 내의 Table 들의 list 를 반환하는 함수
    def get_table_list(self):
        get_table_list_query = """ SELECT name FROM sqlite_master
                                WHERE type='table'
                                ORDER BY name; """
        try:
            c = self.conn.cursor()
            c.execute(get_table_list_query)

            rows = c.fetchall()
            table_list = list()

            for i in range(len(rows)):
                table_list.append(list(rows[i])[0])
            
            c.close()
            return table_list
            
        except Error as e:
            print(e)

    def get_column_list(self, table_name):
        get_column_query = """ PRAGMA table_info(%s); """ % table_name
        try:
            c = self.conn.cursor()
            c.execute(get_column_query)

            rows = c.fetchall()
            column_list = list()

            for i in range(len(rows)):
                column_list.append(list(rows[i])[1])
                
            c.close()

            return column_list

        except Error as e:
            print(e)

    def close_conn(self):
        self.conn.close()

    def insert_database_multi_rows(self,table_name, column_list, rows_values):
        """
                insert a new values into the table
                :param conn: local db connection
                :param table_name: the table which have the target information
                :param column_list: the columns of the table
                :param row_values:
                :return: project id
                """
        try:
            sql = ''' INSERT INTO %s (''' % table_name

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + column_list[i] + ','
                else:
                    sql = sql + column_list[i]

            sql = sql + ') VALUES ('

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + '?,'
                else:
                    sql = sql + '?);'
            # print(sql)
            c = self.conn.cursor()
            c.executemany(sql, rows_values)
            self.conn.commit()
            c.close()
            return c.lastrowid
        except Error as e:
            print(e)


    def insert_non_exist_row_database_multi_rows(self,table_name, column_list, rows_values):
        """
                insert a new values into the table
                :param conn: local db connection
                :param table_name: the table which have the target information
                :param column_list: the columns of the table
                :param row_values:
                :return: project id
                """
        try:
            sql = ''' INSERT OR IGNORE INTO %s (''' % table_name

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + column_list[i] + ','
                else:
                    sql = sql + column_list[i]

            sql = sql + ') VALUES ('

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + '?,'
                else:
                    sql = sql + '?);'
            # print(sql)
            c = self.conn.cursor()
            c.executemany(sql, rows_values)
            self.conn.commit()
            c.close()
            return c.lastrowid
        except Error as e:
            print(e)

    # FIXME: 현재 REPLACE 문은 따로 테스트 해 보지는 않았다. 추후 오류가 발생 되면 여기항목을 수정하기 바람
    def replace_database_multi_rows(self, table_name, column_list, rows_values):
        """
                insert a new values into the table
                :param conn: local db connection
                :param table_name: the table which have the target information
                :param column_list: the columns of the table
                :param row_values:
                :return: project id
                """
        try:
            sql = ''' REPLACE INTO %s (''' % table_name

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + column_list[i] + ','
                else:
                    sql = sql + column_list[i]

            sql = sql + ') VALUES ('

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + '?,'
                else:
                    sql = sql + '?);'

            c = self.conn.cursor()
            c.executemany(sql, rows_values)
            self.conn.commit()
            c.close()
            return c.lastrowid
        except Error as e:
            print(e)

    def replace_database_row(self, table_name, column_list, rows_values):
        """
                insert a new values into the table
                :param conn: local db connection
                :param table_name: the table which have the target information
                :param column_list: the columns of the table
                :param row_values:
                :return: project id
                """
        try:
            sql = ''' REPLACE INTO %s (''' % table_name

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + column_list[i] + ','
                else:
                    sql = sql + column_list[i]

            sql = sql + ') VALUES ('

            for i in range(len(column_list)):
                if i < len(column_list) - 1:
                    sql = sql + '?,'
                else:
                    sql = sql + '?);'

            c = self.conn.cursor()
            c.execute(sql, rows_values)
            self.conn.commit()
            c.close()
            return c.lastrowid
        except Error as e:
            print(e)

    def insert_database_row(self ,table_name, column_list, row_values):
        """
        insert a new values into the table
        :param conn: local db connection
        :param table_name: the table which have the target information
        :param column_list: the columns of the table
        :param row_values:
        :return: project id
        """
        try:
            sql = ''' INSERT INTO %s (''' % table_name

            for i in range(len(column_list)):
                if i < len(column_list)-1:
                    sql = sql + column_list[i] + ','
            else:
                sql = sql + column_list[i]

            sql = sql + ') VALUES ('

            for i in range(len(column_list)):
                if i < len(column_list)-1:
                    sql = sql + '?,'
            else:
                sql = sql + '?);'

            c = self.conn.cursor()
            c.execute(sql, row_values)
            self.conn.commit()
            c.close()
            return c.lastrowid
        except Error as e:
            print(e)

    def insert_database_row_group_commit(self ,table_name, column_list, row_values):
        """
        insert a new values into the table
        :param conn: local db connection
        :param table_name: the table which have the target information
        :param column_list: the columns of the table
        :param row_values:
        :return: project id
        """
        try:
            sql = ''' INSERT INTO %s (''' % table_name

            for i in range(len(column_list)):
                if i < len(column_list)-1:
                    sql = sql + column_list[i] + ','
            else:
                sql = sql + column_list[i]

            sql = sql + ') VALUES ('

            for i in range(len(column_list)):
                if i < len(column_list)-1:
                    sql = sql + '?,'
            else:
                sql = sql + '?);'

            c = self.conn.cursor()
            c.execute(sql, row_values)
            if self.insert_query_numbers < self.query_commit_number:
                self.insert_query_numbers += 1
            else:
                self.conn.commit()
                self.insert_query_numbers = 0
            c.close()
            return c.lastrowid
        except Error as e:
            print(e)

    def select_db(self, select_sql):
        c = self.conn.cursor()
        c.execute(select_sql)
        results = c.fetchall()

        return results


    # TODO: Transform the update format for general purposes.
    def update_database_multirows(self, data_table , set_list, set_value_tuple, where_list, where_value):

        try:
            sql = ''' UPDATE ''' + data_table
            sql = sql + ''' SET '''
            for i in range(len(set_list)):
                if i < len(set_list) - 1:
                    sql = sql + set_list[i] + ' = ? ,'
                else:
                    sql = sql + set_list[i] + ' = ? '

            sql = sql + ''' WHERE '''

            for i in range(len(where_list)):
                if i < len(where_list) - 1:
                    sql = sql + where_list[i] + ' = ? AND '
                else:
                    sql = sql + where_list[i] + ' = ? '
            values = [set_value_tuple + where_value]
            print(sql)
            print(values)

            c = self.conn.cursor()
            c.executemany(sql, values)
            self.conn.commit()
            c.close()

        except Exception as e:
            print(e)

        # cur = conn.cursor()
        # cur.execute(sql, task)
        # conn.commit()

    def update_database_row(self, data_table , set_list, set_value_tuple, where_list, where_value):

        try:
            sql = ''' UPDATE ''' + data_table
            sql = sql + ''' SET '''
            for i in range(len(set_list)):
                if i < len(set_list) - 1:
                    sql = sql + set_list[i] + ' = ? ,'
                else:
                    sql = sql + set_list[i] + ' = ? '

            sql = sql + ''' WHERE '''

            for i in range(len(where_list)):
                if i < len(where_list) - 1:
                    sql = sql + where_list[i] + ' = ? AND '
                else:
                    sql = sql + where_list[i] + ' = ? '
            values = set_value_tuple + where_value
            print(sql)
            print(values)

            c = self.conn.cursor()
            c.execute(sql, values)
            self.conn.commit()
            c.close()

        except Exception as e:
            print(e)

        # cur = conn.cursor()
        # cur.execute(sql, task)
        # conn.commit()
