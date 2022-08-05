#импорт библиотек
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
from datetime import datetime, timedelta
from tabulate import tabulate

#импорт библиотек airflow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = '5575671590:AAHSLefnc-Ivqk9rit5qLUuLAoRoDyg5eeI' # тут нужно заменить на токен вашего бота
bot = telegram.Bot(token=my_token) # получаем доступ
chat_id = 306618482 

def three_sigma_std(data):
    """
    Получаем на вход столбец Series,
    Возвращаем интервал минус три сигма, плюс три сигма
    """
    return ((data.mean()-3*data.std())), (data.mean() + 3*data.std())

def interval_alarm(min_value, max_value, value):
    """
    Возвращаем True или False в зависимости от того,
    попадает ли значение value а интервал (min_value, max_value)
    """
    return (value < min_value) or (value > max_value)

def send_msg(msg, chat_id = 306618482):
    """
    Отправляем сообщение в telegram
    """
    bot.sendMessage(chat_id=chat_id, text=msg)
    
def send_graf(plot_object, chat_id = 306618482):
    """
    Отправляем фото в telegram
    """
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
#строка для соединения с CH
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'v.morozov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 14),
}

# Интервал запуска DAG каждые 15 минут
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def vmorozov_alarm_dag():
        
    @task
    def extract():
        """
        Запрос количества уникальных пользователей за последние 3 часа с разбивкой по 15 минут.
        Последний значение в интервале не берем, так как оно может быть не полным.
        """
        sql_query = """
        SELECT toStartOfFifteenMinutes(time) AS fifteen_minutes,
               countIf(action = 'view') AS Views,
               countIf(action = 'like') AS Likes,
               Likes/Views As CTR,
               COUNT(DISTINCT CASE WHEN action = 'msg' THEN user_id END) AS msg_users,
               COUNT(DISTINCT user_id) - msg_users As feed_users
        FROM (
               SELECT time,
                      user_id,
                      action
               FROM simulator_20220620.feed_actions
               
               UNION ALL 
               
               SELECT time,
                      user_id,
                      'msg' AS action
               FROM simulator_20220620.message_actions
               )
        WHERE time >= date_sub(hour, 4, now()) AND fifteen_minutes <> toStartOfFifteenMinutes(now())
        GROUP BY fifteen_minutes
        ORDER BY fifteen_minutes
        """
        data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return data
    
    
    @task
    def upload(data):
        for column in data.columns[1:]:
            min_value, max_value = three_sigma_std(data[column][:-1])
            if (interval_alarm(min_value, max_value, data[column][-1:].values[0])):
                if data[column][-1:].values[0] < min_value:
                    diff = round((min_value - data[column][-1:].values[0])/min_value*100, 2)
                else:
                    diff = round((data[column][-1:].values[0]-max_value)/max_value*100, 2)
                msg = f"""Метрика {column}, текущее значение {data[column][-1:].values[0]}
                Доверительный интервал от {min_value:.2f} до {max_value:.2f} выходит за пределы интервала более чем на {diff} процентов"""
                plt.figure(figsize=(12, 10))
                sns.set_theme(style="whitegrid")
                sns.lineplot(x = data['fifteen_minutes'].dt.strftime('%H:%M'), y = data[column], label = column)
                sns.lineplot(x = data['fifteen_minutes'].dt.strftime('%H:%M'), y = min_value, label = 'min_value')
                sns.lineplot(x = data['fifteen_minutes'].dt.strftime('%H:%M'), y = max_value, label = 'max_value')
                plt.title(column)
                plt.legend()
                plt.xlabel("Date")
                plt.ylabel("")
                plt.xticks(rotation=90)
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'metric.png'
                plt.close()
                    
                send_msg(msg)
                send_graf(plot_object)
    
    
    #extract
    dag_cube = extract()
    
    #tramnsform
    
    #upload
    upload(dag_cube)
    

    
vmorozov_alarm_dag = vmorozov_alarm_dag()