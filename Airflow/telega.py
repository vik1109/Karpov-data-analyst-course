#импорт библиотек
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
from datetime import datetime, timedelta

#импорт библиотек airflow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = 'тут нужно заменить на токен вашего бота' # тут нужно заменить на токен вашего бота
bot = telegram.Bot(token=my_token) # получаем доступ

#sql - запрос
sql_feed = """SELECT toDate(time) AS event_date,
            countIf(action = 'like') AS likes,
            countIf(action = 'view') AS view,
            likes/view AS ctr,
            uniqExact(user_id) AS dau
            FROM simulator_20220620.feed_actions
            GROUP BY toDate(time) HAVING toDate(time)>=today()-7 AND toDate(time)<today()"""

#строка для соединения с CH
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'password',
    'user': 'student',
    'database': 'simulator'
}
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'v.morozov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 5),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def vmorozov_telegram_dag():
        
    @task
    def extract():
        data = pandahouse.read_clickhouse(sql_feed, connection=connection)
        return data
    
    @task
    def upload(data):
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        likes = data[data['event_date'] == yesterday]['likes'].values[0]
        view = data[data['event_date'] == yesterday]['view'].values[0]
        ctr = data[data['event_date'] == yesterday]['ctr'].values[0]
        dau = data[data['event_date'] == yesterday]['dau'].values[0]
        
        #simple msg
        msg = f"""Отчет за {yesterday}:
        Лайки - {likes} 
        Просмотры - {view}
        CTR - {round(ctr,4)}
        DAU - {dau}"""
        chat_id = 306618482
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        #DAU
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['dau'], label = 'DAU')
        plt.title('DAU (last week)')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'dau.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #CTR
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['ctr'], label = 'CTR')
        plt.title('CTR (last week)')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'ctr.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #likes and views
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['likes'], label = 'likes')
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['view'], label = 'views')
        plt.title('Likes and views (last week)')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'like_view.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #report .csv
        file_object = io.StringIO()
        data.to_csv(file_object)
        file_object.name = 'report.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
    
    #extract
    df_cube = extract()
    
    #transform
    
    #load
    upload(df_cube)
    
vmorozov_telegram_dag = vmorozov_telegram_dag()