import pandas as pd

from datetime import timedelta
from datetime import datetime

import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# определяем год для рассчета метрик с помощью хэш-функции
my_year = 1994 + hash(f'a-somov') % 23

# задаем параметры
default_args = {
    'owner': 'a-somov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 19)
}

schedule_interval = '0 16 * * *'

CHAT_ID = 0

BOT_TOKEN = Variable.get('telegram_secret')

# отправка сообщения в tg при успешном выпронении   
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if CHAT_ID != 0:
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

# с помощью декоратора задаем dag
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_somov_lesson3():
    @task() # считываем данные. фильтруем по необходимому году
    def get_data():
        data = pd.read_csv('vgsales.csv') \
                 .query('Year == @my_year')
        return data


    @task() # самые продаваемые игры в этом году
    def get_bestseller_game(data):
        bestseller_game = data[data.Global_Sales == data.Global_Sales.max()] \
                              .Name.values[0]
        return bestseller_game


    @task() # самые продаваемые жанры в европе
    def get_top_EU_Genre(data):
        top_EU_Genre = data.groupby('Genre', as_index=False) \
                           .agg({'EU_Sales': 'sum'})
        top_EU_Genre = top_EU_Genre[top_EU_Genre.EU_Sales == top_EU_Genre.EU_Sales.max()] \
                                   .Genre.to_list()
        return top_EU_Genre


    @task() # топ платформ в NA, проданных более чем милионным тиражом
    def get_top_NA_Platform(data):
        top_NA_Platform = data.query('NA_Sales > 1') \
                              .groupby('Platform', as_index=False) \
                              .agg({'Name': 'count'}) \
                              .rename(columns={'Name': 'Number'})
        top_NA_Platform = top_NA_Platform[top_NA_Platform.Number == top_NA_Platform.Number.max()] \
                                         .Platform.to_list()
        return top_NA_Platform


    @task() # издатель с самыми высокими средними продажами в японии
    def get_top_JP_Publisher(data):
        top_JP_Publisher = data.groupby('Publisher', as_index=False) \
                               .agg({'JP_Sales': 'mean'})
        top_JP_Publisher = top_JP_Publisher[top_JP_Publisher.JP_Sales == top_JP_Publisher.JP_Sales.max()] \
                                           .Publisher.to_list()
        return top_JP_Publisher


    @task # игры, проданные в Европе большим тиражом, чем в Японии
    def get_EU_more_JP(data):
        EU_more_JP =  data.query('EU_Sales > JP_Sales').shape[0]
        return EU_more_JP

    @task(on_success_callback=send_message)
    def print_data(bestseller_game, top_EU_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP):

        context = get_current_context()
        date = context['ds']

        print(f'''Data for {my_year} for {date}
                  Bestseller Global Game: {bestseller_game}
                  Top EU Genres: {top_EU_Genre}
                  Top NA Platforms: {top_NA_Platform}
                  Top JP Publishers: {top_JP_Publisher}
                  The number of games with sales in the EU is greater than in JP: {EU_more_JP}''')

    # задаем последовательность тасков
    data = get_data()

    bestseller_game = get_bestseller_game(data)
    top_EU_Genre = get_top_EU_Genre(data)
    top_NA_Platform = get_top_NA_Platform(data)
    top_JP_Publisher = get_top_JP_Publisher(data)
    EU_more_JP = get_EU_more_JP(data)

    print_data(bestseller_game, top_EU_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP)

a_somov_lesson3 = a_somov_lesson3()