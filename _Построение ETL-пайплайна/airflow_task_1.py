import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10_domains_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.').str[-1]
    top_10_domains = top_data_df.domain_zone.value_counts().head(10)

    with open('top_10_domains_zone.csv', 'w') as f:
        f.write(top_10_domains.to_csv(header=False))


def top_length_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    top_length_domain = top_data_df.sort_values(['length', 'domain'], ascending=(False, True)) \
                                   .reset_index(drop=True) \
                                   .loc[0, 'domain']

    with open('top_length_domain.csv', 'w') as f:
        f.write(top_length_domain)


def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df[top_data_df.domain == 'airflow.com'].shape[0]:
        airflow_rank = top_data_df[top_data_df.domain == 'airflow.com'].loc[0, 'rank']
    else:
        airflow_rank = 'airflow.com not in top_domains'
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank)


def print_data(ds):
    date = ds

    with open('top_10_domains_zone.csv', 'r') as f:
        top_domains = f.read()

    with open('top_length_domain.csv', 'r') as f:
        top_length = f.read()

    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()

    print(f'Top domains for date {date}')
    print(top_domains)

    print(f'Top length domain for date {date}')
    print(top_length)

    print(f'airflow.com rank for date {date}')
    print(airflow_rank)

default_args = {
    'owner': 'a-somov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 19),
    'schedule_interval': '0 16 * * *'
}


dag = DAG('a-somov_lesson2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domains_zone',
                    python_callable=top_10_domains_zone,
                    dag=dag)

t3 = PythonOperator(task_id='top_length_domain',
                        python_callable=top_length_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5