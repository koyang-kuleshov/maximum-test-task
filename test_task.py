'''Скачивает данные со Postgres и готовит сводную таблицу'''
import os

import pandas as pd
import psycopg2


HOST = os.environ['POSTGRES_HOST']
PORT = os.environ['POSTGRES_PORT']
USER = os.environ['POSTGRES_USER']
PASS = os.environ['POSTGRES_PASS']
DB = os.environ['POSTGRES_DB']


def create_connection(db_host, db_port, db_user, db_password, db_name):
    '''Создает соединении с базой данных.

    Args:
        db_host : |str| Адрес сервера
        db_port : |int| Порт сервера
        db_user : |str| Имя пользователя
        db_password : |str| Пароль
        db_name : |str| Имя БД
    '''
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            )
    except psycopg2.OperationalError as err:
        print(err)
    return connection


class MaximumTask:
    '''Обрабатывает полученные данные двумя способами и сравнивает
    полученные решения друг с другом.'''
    def __init__(self):
        self.connection = create_connection(HOST, PORT, USER, PASS, DB)
        self.result_df = pd.DataFrame()
        self.result_psql = pd.DataFrame()

    def execute_query(self, query):
        '''Отправляет sql-запрос и возвращает ответ от БД.

        Args:
            query : |str| SQL-запрос
        '''
        cursor = self.connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except psycopg2.OperationalError as err:
            print(err)
        return None

    def do_task_with_pandas(self):
        '''Получает данные из базы и обрабатывает их с помощью pandas.'''
        sessions_columns = [
            'visitor_session_id',
            'site_id',
            'visitor_id',
            'date_time',
            'campaign_id'
        ]
        communications_columns = [
            'communication',
            'site_id',
            'visitor_id',
            'date_time'
        ]
        get_sessions_query = 'SELECT * FROM sessions;'
        get_communications_query = 'SELECT * FROM communications;'
        sessions = self.execute_query(get_sessions_query)
        communications = self.execute_query(get_communications_query)

        sessions_df = pd.DataFrame(data=sessions, columns=sessions_columns)
        communications_df = pd.DataFrame(
            data=communications,
            columns=communications_columns)

        merged_df = pd.merge(
            communications_df,
            sessions_df,
            on='visitor_id',
            how='left',
            suffixes=['_com', '_ses']
        )
        merged_df = merged_df.query(
            'site_id_com == site_id_ses and date_time_com > date_time_ses'
        ).sort_values(['communication', 'date_time_ses'])
        merged_df['row_n'] = merged_df.sort_values(
            'date_time_ses').groupby('communication').cumcount() + 1
        merged_df.row_n = merged_df.row_n.astype('Int64')
        merged_df = merged_df.sort_values(
            'communication').reset_index(drop=True)
        merged_df.drop(['site_id_ses'], axis=1, inplace=True)
        merged_df.rename(
            {'communication': 'communication_id',
                'site_id_com': 'site_id',
                'visitor_id_x': 'visitor_id',
                'date_time_com': 'communication_date_time',
                'date_time_ses': 'session_date_time',
                'communication_y': 'row_n'
             },
            axis='columns',
            inplace=True
        )

        for comm in merged_df.communication_id.unique():
            spam = pd.DataFrame()
            spam = merged_df.query(
                'communication_id == @comm'
            ).sort_values('session_date_time', ascending=False).head(1)
            self.result_df = pd.concat([self.result_df, spam])
        self.result_df = self.result_df.reset_index(drop=True)
        print(
            'result_df info'
            f'{self.result_df.info}',
            f'{self.result_df.head(5)}',
            sep='\n')

    def do_task_with_sql(self):
        '''Получает данные из БД, приводит к нужным типам данных.'''
        psql = """
        WITH merged_table AS (
            SELECT
                c.communication_id AS communication_id,
                c.site_id AS site_id,
                c.visitor_id AS visitor_id,
                c.date_time AS communication_date_time,
                s.visitor_session_id::BIGINT AS visitor_session_id,
                s.date_time AS session_date_time,
                s.campaign_id AS campaign_id,
                ROW_NUMBER() over(PARTITION BY c.communication_id ORDER BY
                (s.date_time)) AS row_n
            FROM
                communications AS c
            LEFT JOIN
                sessions AS s
            ON
                c.visitor_id = s.visitor_id
            WHERE
                c.site_id = s.site_id
                AND c.date_time > s.date_time
            ORDER BY
                communication_id
            )
        SELECT
            mt.communication_id,
            mt.site_id,
            mt.visitor_id,
            mt.communication_date_time,
            CASE
                WHEN sr.session_date_time IS NOT NULL THEN mt.visitor_session_id
            END AS visitor_session_id,
            sr.session_date_time,
            CASE
                WHEN sr.session_date_time IS NOT NULL THEN mt.campaign_id
            END AS campaign_id,
            sr.max_n AS row_n
        FROM
            merged_table AS mt
        LEFT JOIN
            (
            SELECT
                communication_id,
                MAX(row_n) AS max_n,
                MAX(session_date_time) AS session_date_time
            FROM
                merged_table
            GROUP BY
                communication_id
            ORDER BY
                communication_id
            ) AS sr
        ON
            mt.communication_id = sr.communication_id
        WHERE
            mt.row_n = sr.max_n
        """
        execute_psql = self.execute_query(psql)
        self.result_psql = pd.DataFrame(
            execute_psql, columns=self.result_df.columns.to_list())
        self.result_psql.visitor_session_id = (
            self.result_psql.visitor_session_id.astype('Int64'))
        self.result_psql.campaign_id = (
            self.result_psql.campaign_id.astype('Int64'))
        print(
            'result_psql info'
            f'{self.result_psql.info()}',
            f'{self.result_psql.head(5)}',
            sep='\n')

    @property
    def compare_results(self):
        '''Сравнивает 2 резултата и выводит сообщение.'''
        diff = self.result_df.compare(self.result_psql)
        if diff.empty:
            print('Датафреймы идентичны')
        else:
            print('Датафреймы различны')


if __name__ == '__main__':
    test_task = MaximumTask()
    test_task.do_task_with_pandas()
    test_task.do_task_with_sql()
    test_task.compare_results
