import pymysql, pandas
from time import sleep


class MariaDbQueryManager(object):
    def __init__(self, host=None, port=3306, default_database='', username=None, password=None):
        try:
            assert None not in [host, username, password]
        except AssertionError:
            if host is None:
                print('[MariaDbQueryManager] `host` must be specified.')
            elif username is None:
                print('[MariaDbQueryManager] `username` must be specified.')
            elif password is None:
                print('[MariaDbQueryManager] `password` must be specified.')

        self.host = host
        self.port = port
        self.default_database = default_database
        self.username = username
        self.password = password
        self.connection = None

        self.renew_db_connection()  # test connection
        self.close_db_connection()  # test connection

    def close_db_connection(self, verbose=True):
        if self.connection is not None:
            # commit connection
            try:
                self.connection.commit()
            except Exception as e:
                if verbose:
                    print(e)
                pass

            # close connection
            try:
                self.connection.close()
            except Exception as e:
                if verbose:
                    print(e)
                pass

        self.connection = None

    def renew_db_connection(self, default_database=None):
        self.close_db_connection()

        if default_database is not None:
            if self.default_database != default_database:
                print(f'[MariaDbQueryManager] `default_database` is changed to `{default_database}`.'.format(default_database))
            self.default_database = default_database

        max_retry_counts = 10
        for retry_count in range(max_retry_counts):
            try:
                self.connection = pymysql.connect(host=self.host, port=self.port, user=self.username, password=self.password, db=self.default_database)
                break
            except (OSError, pymysql.err.OperationalError) as e:
                print('[MariaDbQueryManager] `renew_db_connection()`', e)
                print('exception occurred! reconnect database (%d of %d)' % (retry_count + 1, max_retry_counts))
                sleep(20 + retry_count)

    def check_table_existence(self, table, database=None):
        database = self.default_database if database is None else database

        query = 'DESCRIBE `{database}`.`{table}`'.format(database=database, table=table)

        self.renew_db_connection()

        try:
            self.execute_then_fetch_sql(query)
            existence = True
        except Exception as e:
            existence = False
        finally:
            self.close_db_connection()

        return existence

    def execute_sql(self, query):
        self.renew_db_connection()

        cursor = self.connection.cursor()
        response = cursor.execute(query)

        self.close_db_connection()

        return response

    def execute_then_fetch_sql(self, query, show_header=True):
        self.renew_db_connection()

        cursor = self.connection.cursor()
        cursor.execute(query)
        response = cursor.fetchall()

        self.close_db_connection()

        # num_fields = len(cursor.description)
        if show_header:
            header = tuple(i[0] for i in cursor.description)  # column names
            return (header, ) + response
        else:
            return response

    def get_dataframe(self, table, database=None):
        database = self.default_database if database is None else database

        sql = 'SELECT * FROM `{database}`.`{table}`'.format(database=database, table=table)

        self.renew_db_connection()
        fetched = self.execute_then_fetch_sql(sql)
        self.close_db_connection()

        fetched = pandas.DataFrame(fetched)
        fetched = fetched.rename(columns=fetched.iloc[0]).drop(fetched.index[0])

        return fetched

    def insert_dataframe(self, dataframe, table, data_types=dict(), database=None, drop_if_exists=True):
        database = self.default_database if database is None else database

        assert type(dataframe) is pandas.DataFrame
        dataframe = dataframe.astype('string')  # default data type: string
        for column_name in data_types:  # explicit data type by column
            dataframe[column_name] = dataframe[column_name].astype(data_types[column_name])

        column_definition = ''
        for column_name in dataframe.columns.to_list():
            if len(column_definition) > 0:
                column_definition += ', '

            column_definition += '`{column_name}` {data_type}'.format(
                column_name=column_name, data_type=data_types[column_name] if column_name in data_types else 'TEXT'
            )

        if drop_if_exists:
            if self.check_table_existence(table, database=database):
                self.purge_table(table, database=database)

        # create table
        sql = 'CREATE TABLE `{database}`.`{table}` ({column_definition})'.format(database=database, table=table, column_definition=column_definition)
        self.execute_sql(sql)

        # insert dataframe
        sql = 'INSERT IGNORE INTO `{database}`.`{table}` ({column_names}) VALUES ({column_values_placeholder})'.format(
            database=database, table=table, column_names='`' + '`, `'.join(dataframe.columns.to_list()) + '`', column_values_placeholder=('%s ' * len(dataframe.columns.to_list())).strip().replace(' ', ', ')
        )

        self.renew_db_connection()

        with self.connection.cursor() as cursor:
            max_retry_counts = 10
            for retry_count in range(max_retry_counts):
                try:
                    cursor.executemany(sql, dataframe.values.tolist())
                    break
                except (OSError, pymysql.err.OperationalError) as e:
                    print('[MariaDbQueryManager] `insert_dataframe()`', e)
                    print('exception occurred! reconnect database (%d of %d)' % (retry_count + 1, max_retry_counts))
                    sleep(20 + retry_count)

                    self.renew_db_connection()

        self.close_db_connection()

    def purge_table(self, table, database=None):
        database = self.default_database if database is None else database
        self.execute_sql('DROP TABLE IF EXISTS `{database}`.`{table}`'.format(database=database, table=table))


if __name__ == '__main__':
    mariadb_query_manager = MariaDbQueryManager(host='', port=3306, username='', password='', default_database='')
    test_df = mariadb_query_manager.get_dataframe('test')
    mariadb_query_manager.insert_dataframe(test_df, 'test')
