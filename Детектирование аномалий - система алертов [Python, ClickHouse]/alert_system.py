import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import telegram
from read_CH import Getch

import io
import sys
import os


def check_anomaly(df, metric, a=3.5, n=5, threshold=0.2):
    # Воспользуемся комбинированным методом определения аномалий, первый - статистический (3std),
    # второй - сравнение с предыдущим днем
    
    # Статистический метод
    df['rmean_n'] = df[metric].shift(1).rolling(n).mean()
    df['std_n']   = df[metric].shift(1).rolling(n).std()

    df['up_std']  = df['rmean_n'] + a * df['std_n']
    df['low_std'] = df['rmean_n'] - a * df['std_n']
    
    # Сравнение с предыдущим днем
    df['up_1dago'] = df[metric] + threshold * df[metric].shift(96) # в сутках 96 15-ти минуток
    df['low_1dago'] = df[metric] - threshold * df[metric].shift(96)
    
    # Рассчет границ для постройки графика
    df['up'] = df[['up_1dago', 'up_std']].mean(axis=1)
    df['low'] = df[['low_1dago', 'low_std']].mean(axis=1)
    # Сгладим        
    df['up']  = df['up'].rolling(n, center=True).mean()
    df['low']  = df['low'].rolling(n, center=True).mean()
    
    # Двусторонняя проверка
    is_alert = 0
    if df[metric].iloc[-1] < df['low_std'].iloc[-1] and df[metric].iloc[-1] < df['low_1dago'].iloc[-1] or \
    df[metric].iloc[-1] > df['up_std'].iloc[-1] and df[metric].iloc[-1] > df['up_1dago'].iloc[-1]:
    # if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1

    return is_alert, df


def draw_plot(df, metric):
    
    metric_parametrs = {'users_feed':
                                {'title': 'Активные пользователи [Feed]'},
                        'views':
                                {'title': 'Просмотры [Feed]'}, 
                        'likes':
                                {'title': 'Лайки [Feed]'}, 
                        'ctr':
                                {'title': 'Конверсия просмотра в лайк [Feed]'}, 
                        'users_message':
                                {'title': 'Активные пользователи [Msg]'}, 
                        'number_messages':
                                {'title': 'Количество сообщений [Msg]'}}
    
    title = metric_parametrs[metric]['title']
    
    sns.set(style = 'whitegrid',
            rc={'figure.figsize': (16, 10)})
    plt.tight_layout()

    ax = sns.lineplot(data=df.iloc[-193:], x='ts', y=metric, label='metric')
    ax = sns.lineplot(data=df.iloc[-193:], x='ts', y='up', label='up')
    ax = sns.lineplot(data=df.iloc[-193:], x='ts', y='low', label='low')

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 1 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)

    ax.set(xlabel='time', ylabel=None)
    ax.set_title(title)
    if metric != 'ctr':
        ax.set(ylim=(0, None))

    plot_object = io.BytesIO()
    ax.figure.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'{metric}.png'
    plt.close()
    
    return plot_object, title


def run_alerts(chat=None):
    chat_id = chat or 58150305 
    bot = telegram.Bot(token=os.environ.get('tg_bot_token'))
    
    data_feed = Getch('''SELECT
                      toStartOfFifteenMinutes(time) as ts
                    , toDate(time) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_feed
                    , countIf(user_id, action = 'view') as views
                    , countIf(user_id, action = 'like') as likes
                    , countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as ctr
                FROM simulator_20220520.feed_actions
                WHERE time >= yesterday() - 1  and
                      time < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts''').df

    data_msg = Getch('''SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(time) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as users_message
                        , COUNT(user_id) as number_messages
                    FROM simulator_20220520.message_actions
                    WHERE time >= yesterday() -1 and
                          time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts''').df

    data_list = [data_feed, data_msg]
    metric_list = [['users_feed', 'views', 'likes', 'ctr'], ['users_message', 'number_messages']]
    
    msg = '{metric}:\n\nТекущее значение {current_value:.2f}\nОтклонение от предыдущего значения {diff:.2%}' + \
    '\n<a href="https://superset.lab.karpov.courses/superset/dashboard/991/">Ссылка на дашборд</a>' +\
    '\n@adagudeda приди'
    
    for data, metric_l in zip(data_list, metric_list):
        for metric in metric_l:
            df_stat = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df_stat, metric)
            if is_alert:
                plot_object, title = draw_plot(df, metric)
                caption = msg.format(metric = title,
                                     current_value = df[metric].iloc[-1],
                                     diff = 1 - df[metric].iloc[-1]/df[metric].iloc[-2])                
                bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=caption, parse_mode='HTML')
    

run_alerts(-652068442) #-652068442