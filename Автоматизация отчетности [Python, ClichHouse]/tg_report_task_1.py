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


def tg_report(chat_id=-715060805):

    # Вчерашний день и неделя назад для отеча
    yesterday = (dt.date.today()-dt.timedelta(days=1)).strftime('%Y-%m-%d')
    week_before = (dt.date.today()-dt.timedelta(days=7)).strftime('%Y-%m-%d')

    # Создаем бота
    bot = telegram.Bot(token=os.environ.get('tg_bot_token'))

    # За вчерашний день
    data_yesterday = Getch(f'''SELECT 
                    COUNT(DISTINCT user_id) AS DAU,
                    countIf(user_id, action='view') as Views,
                    countIf(user_id, action='like') as Likes,
                    countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
                 FROM simulator_20220520.feed_actions 
                 WHERE toDate(time) = yesterday()''').df

    # Текстовый отчет за вчерашний день
    report_yesterday = f'''Ключевые метрики за {yesterday}
    DAU: {data_yesterday.DAU[0]}
    Просмотры: {data_yesterday.Views[0]}
    Лайки: {data_yesterday.Likes[0]}
    CTR: {data_yesterday.CTR[0].round(4)}'''

    # Метрики за неделю
    data_week = Getch(f'''SELECT 
                    toDate(time) as Date,
                    COUNT(DISTINCT user_id) AS DAU,
                    countIf(user_id, action='view') as Views,
                    countIf(user_id, action='like') as Likes,
                    countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
                 FROM simulator_20220520.feed_actions
                 WHERE Date <= yesterday()
                      and Date > date_sub(DAY, 7, yesterday())
                 GROUP BY Date''').df

    #  Графики распрделения сумм покупок по группам
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(20, 10))

    titles = ['DAU for last 7d', 'CTR for last 7d', 'Likes for last 7d', 'Views for last 7d']
    metrics = ['DAU', 'CTR', 'Likes', 'Views']
    colors = ['coral', 'blue', 'green', 'purple']

    for t, m, ax, col in zip(titles, metrics, axes.flatten(), colors):
        sns.lineplot(data=data_week, ax=ax, x='Date', y=m, color=col, markers=True)
        ax.set(title=t, xlabel=None)

    # Отправляем в tg
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'Report for{week_before} - {yesterday}.png'
    plt.close()

    bot.send_message(chat_id, text=report_yesterday)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=f'Report for {week_before} - {yesterday}')
    
try:
    tg_report()
except Exception as e:
    print(e)