from datetime import datetime, timedelta, date
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task

default_args = {
    'owner': 'a-somov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 6),
}

schedule_interval = '0 11 * * *'

# Вчерашний день
yesterday = (date.today() - timedelta(days=1))

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_tasks():

    @task()
    def load_feed():
        query = '''
        SELECT 
             user_id
            ,sum(action='view') as views
            ,sum(action='like') as likes
            ,if(min(gender)=1, 'male', 'female') as gender
            ,min(os) as os
            ,multiIf(min(age) < 18, '<18', 
                     min(age) >= 18 and min(age) < 25, '18-25', 
                     min(age) >= 25 and min(age) < 35, '25-35', 
                     min(age) >= 35 and min(age) < 50, '35-50', 
                     '>=55') as age
        FROM simulator_20220520.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id
        '''

        data_feed = ph.read_clickhouse(query, connection=connection)
        
        return data_feed

    @task()
    def load_msg():
        query = '''
        SELECT * 
        FROM
            (SELECT 
                 user_id
                ,count(reciever_id) as messages_sent
                ,count(DISTINCT reciever_id) as users_sent
                ,if(min(gender)=1, 'male', 'female') as gender
                ,min(os) as os
                ,multiIf(min(age) < 18, '<18', 
                         min(age) >= 18 and min(age) < 25, '18-25', 
                         min(age) >= 25 and min(age) < 35, '25-35', 
                         min(age) >= 35 and min(age) < 50, '35-50', 
                         '>=55') as age
            FROM simulator_20220520.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id) as l
            JOIN
                (SELECT
                 reciever_id
                ,count(user_id) as messages_received
                ,count(DISTINCT user_id) as users_received
                FROM simulator_20220520.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY reciever_id) as r
            ON l.user_id = r.reciever_id
                '''

        data_msg = ph.read_clickhouse(query, connection=connection)
        
        return data_msg

    @task()
    def union_tables(data_feed, data_msg):
        union_data = data_msg.merge(data_feed, how='outer', on=['user_id', 'gender', 'os', 'age'])
        return union_data

    @task()
    def context_by_metric(union_data, metric_list=['os', 'age', 'gender']):
        final_data = pd.DataFrame()
        
        for metric in metric_list:
            metric_data = union_data.groupby(metric, as_index=False).agg({'messages_sent': 'sum',
                                                                          'users_sent': 'sum',
                                                                          'messages_received': 'sum',
                                                                          'users_received': 'sum',
                                                                          'views': 'sum',
                                                                          'likes': 'sum'})

            metric_data['metric'] = metric
            metric_data.rename(columns={metric: 'metric_value'}, inplace=True)
            final_data = pd.concat([final_data, metric_data])
        
        return final_data


    @task()
    def save_table(final_data):
        
        # Подготовим таблицу. Добавим дату и преобразуем тип данных        
        final_data['event_date'] = yesterday
        final_data = final_data[['event_date', 'metric', 'metric_value', 'views',
                                 'likes', 'messages_received',
                                 'messages_sent', 'users_received', 'users_sent']]
        
        final_data = final_data.astype({'messages_sent': 'int64',
                                        'users_sent': 'int64',
                                        'messages_received': 'int64',
                                        'users_received': 'int64',
                                        'views': 'int64',
                                        'likes': 'int64'})
        # Запрос на создание, если еще не создана
        create_table = '''
        CREATE TABLE IF NOT EXISTS test.airflow_lesson_asomov
            (event_date Date,
             metric String,
             metric_value String,
             views Int64,
             likes Int64,
             messages_received Int64,
             messages_sent Int64,
             users_received Int64,
             users_sent Int64
        ) ENGINE = Log()'''
        ph.execute(query=create_table, connection=connection_test)
        ph.to_clickhouse(df=final_data, table='airflow_lesson_asomov', index=False, connection=connection_test)

    data_feed = load_feed()
    data_msg = load_msg()
    union_data = union_tables(data_feed, data_msg)
    final_data = context_by_metric(union_data)
    save_table(final_data)

dag_etl_tasks = dag_etl_tasks()

