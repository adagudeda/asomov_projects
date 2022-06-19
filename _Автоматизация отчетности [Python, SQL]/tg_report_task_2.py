import pandas as pd

import telegram
import io
import os
import datetime as dt

from read_CH import Getch

import matplotlib.pyplot as plt
import seaborn as sns

sns.set(
    font_scale=1,
    style="whitegrid",
    rc={'figure.figsize':(15,15)}
        )

# -715060805 58150305
def tg_report(chat=None):
    
    chat_id = chat or 58150305

    # Вчерашний день, неделя и 30д назад для отеча
    yesterday = (dt.date.today()-dt.timedelta(days=1)).strftime('%Y-%m-%d')
    week_before = (dt.date.today()-dt.timedelta(days=7)).strftime('%Y-%m-%d')
    month_before = (dt.date.today()-dt.timedelta(days=30)).strftime('%Y-%m-%d')

    # Создаем бота
    bot = telegram.Bot(token=os.environ.get('tg_bot_token'))


    
    # Топ 10 постов за последние 30 дней в файл
    top_post_yday = Getch(f'''SELECT 
                           post_id AS post_id,
                           count(DISTINCT user_id) AS Coverage,
                           count(user_id) AS Views,
                           countIf(user_id, action='like') AS Likes,
                           countIf(user_id, action='like') / countIf(user_id, action='view') as CTR,
                           countIf(user_id, source='ads') AS AD_traffic
                        FROM simulator_20220520.feed_actions
                        WHERE toDate(time) <= yesterday()
                          and toDate(time) > date_sub(DAY, 30, yesterday())
                        GROUP BY post_id
                        ORDER BY Coverage DESC
                        LIMIT 10''').df
    
    file_object = io.StringIO()
    top_post_yday.to_csv(file_object, index=False)
    file_object.name = f'top10_posts_{month_before} - {yesterday}.csv'
    file_object.seek(0)

    
    # Количество отправленных сообщений и количество пользователей сервиса сообщений last 30d
    message_stat = Getch(f'''SELECT 
                           toDate(time) as Date,
                           COUNT(DISTINCT user_id) as DAU_msg,
                           COUNT(user_id) as msg_number
                        FROM simulator_20220520.message_actions
                        WHERE Date <= yesterday()
                          and Date > date_sub(DAY, 30, yesterday())
                        GROUP BY Date''').df
    
    
    # Статистика по тем, кто пользуется feed+msg
    feed_msg_stat = Getch(f'''SELECT 
                                toDate(time) as Date,
                                COUNT(DISTINCT user_id) AS DAU,
                                countIf(user_id, action='view') as Views,
                                countIf(user_id, action='like') as Likes,
                                countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
                               FROM
                                 (SELECT *
                                  FROM simulator_20220520.feed_actions
                                  WHERE Date <= yesterday()
                                  and Date > date_sub(DAY, 30, yesterday())) as t1
                               JOIN
                                 (SELECT DISTINCT user_id
                                  FROM simulator_20220520.message_actions
                                  WHERE Date <= yesterday()
                                    and Date > date_sub(DAY, 30, yesterday())) as t2 USING user_id
                               GROUP BY Date''').df
    
    
    # Статистика по тем, кто пользуется only feed
    feed_only_stat = Getch(f'''SELECT 
                                toDate(time) as Date,
                                COUNT(DISTINCT user_id) AS DAU,
                                countIf(user_id, action='view') as Views,
                                countIf(user_id, action='like') as Likes,
                                countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
                               FROM
                                 (SELECT *
                                  FROM simulator_20220520.feed_actions
                                  WHERE Date <= yesterday()
                                    and Date > date_sub(DAY, 30, yesterday())) as t1
                               LEFT ANTI JOIN
                                 (SELECT DISTINCT user_id
                                  FROM simulator_20220520.message_actions
                                  WHERE Date <= yesterday()
                                    and Date > date_sub(DAY, 30, yesterday())) as t2 USING user_id
                               GROUP BY Date''').df
    
    
    # Статистика по сервису ссобщений - графики
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(14, 10), constrained_layout=True)

    titles = ['\nMessage service DAU\n', '\nNumber of messages sent\n']
    metrics = ['DAU_msg', 'msg_number']
    colors = ['coral', 'green']

    for t, m, ax, col in zip(titles, metrics, axes.flatten(), colors):
        sns.lineplot(data=message_stat, ax=ax, x='Date', y=m, color=col, markers=True)
        ax.set(title=t, xlabel=None, ylabel=None)
        ax.tick_params(axis='x', rotation=25)

    # Отправляем статистику по сервису сообщений в tg
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'Report for{month_before} - {yesterday}.png'
    plt.close()
    
    # Статистика по ленте в разреге по группам пользователей - графики
    fig_1, axes_1 = plt.subplots(nrows=4, ncols=1, figsize=(14, 20), constrained_layout=True)

    titles_1 = ['\nDAU\n', '\nCTR\n', '\nLikes\n', '\nViews\n']
    metrics_1 = ['DAU', 'CTR', 'Likes', 'Views']

    for t, m, ax in zip(titles_1, metrics_1, axes_1.flatten()):
        sns.lineplot(data=feed_only_stat, ax=ax, x='Date', y=m)
        sns.lineplot(data=feed_msg_stat, ax=ax, x='Date', y=m)
        ax.set(title=t, xlabel=None, ylabel=None) # Задаем заголовки
        ax.tick_params(axis='x', rotation=25) # Поворот оси
        ax.legend(title='User category', labels=['Feed only users', 'Feed&msg users']) # Добовляем легенду
    
    
    # Отправляем статистику по ленте в tg
    plot_object_1 = io.BytesIO()
    plt.savefig(plot_object_1)
    plot_object_1.seek(0)
    plot_object_1.name = f'Report for{month_before} - {yesterday}.png'
    plt.close()
 

    bot.sendDocument(chat_id=chat_id, document=file_object)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=f'Report for {month_before} - {yesterday}. Message service')
    bot.sendPhoto(chat_id=chat_id, photo=plot_object_1, caption=f'Report for {month_before} - {yesterday}. Feed service')

    
try:
    tg_report(chat=-715060805)
except Exception as e:
    print(e)