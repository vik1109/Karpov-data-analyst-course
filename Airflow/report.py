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

my_token = 'тут нужно заменить на токен вашего бота' # тут нужно заменить на токен вашего бота
bot = telegram.Bot(token=my_token) # получаем доступ
chat_id = 306618482 

#sql запросы
#1 - среднее число постов, просмотрови и сообщений на по  льзователя по дням.
#2 - DAU feed and msg
#3 - 1-day, 7-day, 28-day retention
#4 - трафик по источникам (organic and ads)
#5 - общий график за неделю, лайкиб просмотры, сообщения, DAU по дням 
#sql - запрос


def send_msg(msg, chat_id = 306618482):
    bot.sendMessage(chat_id=chat_id, text=msg)
    
def send_graf(plot_object, chat_id = 306618482):
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
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
def vmorozov_report_telegram_dag():
        
    @task
    def extract_first_sql_str():
        sql_query = """SELECT *
        FROM 
        (SELECT toDate(time) AS event_date,
            uniq(user_id) AS feed_user,
            countIf(action = 'view') AS views,
            countIf(action = 'like') AS likes,
            likes/views AS ctr,
            views/feed_user AS mean_views,
            likes/feed_user AS mean_likes,
            uniq(post_id) AS post_count,
            post_count/feed_user AS mean_post
        FROM simulator_20220620.feed_actions
        GROUP BY event_date) frst
        FULL JOIN
        (SELECT uniq(user_id) AS msg_user,
            COUNT(user_id) AS msg,
            toDate(time) AS event_date,
            msg/msg_user AS msg_mean
        FROM simulator_20220620.message_actions
        GROUP BY event_date) scnd
        USING event_date
        WHERE event_date>= today()-28 AND event_date < today()
        """
        first_data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return first_data
    
    @task
    def extract_new_users():
        sql_query = """SELECT count(user_id) AS users, start_date, grp
        FROM(
        SELECT user_id,
            MIN(toDate(time)) AS start_date,
            'feed' AS grp
        FROM simulator_20220620.feed_actions
        GROUP BY user_id
        UNION ALL
        SELECT user_id,
            MIN(toDate(time)) AS start_date,
            'msg' AS grp
        FROM simulator_20220620.message_actions
        GROUP BY user_id
        )
        WHERE start_date>=today()-28
        GROUP BY start_date, grp"""
        data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return data
    
    @task
    def extract_new_gone_retained():
        sql_query ="""SELECT toString(this_week) AS this_week,
                sumIf(num_users, status = 'new') AS new,
                sumIf(num_users, status = 'retained') AS retained,
                sumIf(num_users, status = 'gone') AS gone
                FROM(
                SELECT this_week, previous_week, -uniq(user_id) as num_users, status FROM
                
                (SELECT user_id, 
                groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
                addWeeks(arrayJoin(weeks_visited), +1) this_week, 
                if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status, 
                addWeeks(this_week, -1) as previous_week
                FROM simulator_20220620.feed_actions
                where toDate(time)>=today()-60
                group by user_id)
                
                where status = 'gone'
                
                group by this_week, previous_week, status
                
                HAVING this_week != addWeeks(toMonday(today()), +1)
                
                union all
                
                SELECT this_week, previous_week, toInt64(uniq(user_id)) as num_users, status FROM
                
                (SELECT user_id, 
                groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
                arrayJoin(weeks_visited) this_week, 
                if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
                addWeeks(this_week, -1) as previous_week
                FROM simulator_20220620.feed_actions
                WHERE toDate(time)>=today()-60
                group by user_id)
                
                group by this_week, previous_week, status)
                GROUP BY this_week
                ORDER BY this_week
                """
        
        data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return data
    
    @task
    def extract_top_posts():
        sql_query = """
        SELECT post_id,
               countIf(action ='like') AS like,
               countIf(action = 'view') AS view,
               count(user_id) AS users
        FROM simulator_20220620.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY post_id
        ORDER BY like DESC
        LIMIT 10"""
        data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return data
    
    @task
    def extract_top_city():
        sql_query = """SELECT city,
                       uniq(user_id) AS users
                       FROM simulator_20220620.feed_actions
                       WHERE toDate(time) = yesterday()
                       GROUP BY city
                       ORDER BY users DESC
                       LIMIT 5"""
        data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return data
    
    @task
    def extract_trafic():
        sql_query = """
        SELECT toString(day) AS date,
               sumIf(msg_users, source = 'ads') AS msg_ads,
               sumIf(msg_users, source = 'organic') AS msg_organic,
               sumIf(feed_users, source = 'ads') AS feed_ads,
               sumIf(feed_users, source = 'organic') AS feed_organic
        FROM (SELECT source,
                     uniq(user_id) AS feed_users,
                     toDate(time) AS day
              FROM simulator_20220620.feed_actions
              WHERE toDate(time) >= today()- 28
              GROUP BY source, day) t1
              
              JOIN
              (SELECT source,
                      uniq(user_id) AS msg_users,
                      toDate(time) AS day
               FROM simulator_20220620.message_actions
               WHERE toDate(time) >= today()- 28 AND toDate(time)<today()
               GROUP BY source, day) t2
               USING day, source
        GROUP BY date
        ORDER BY day"""
        data = pandahouse.read_clickhouse(sql_query, connection=connection)
        return data
    
    @task
    def transform_mean_values(data):
        #mean likes, views, msg per User by days
        plt.figure(figsize=(12, 10))
        sns.set_theme(style="whitegrid")
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['mean_views'], label = 'mean_views')
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['mean_likes'], label = 'mean_likes')
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['msg_mean'], label = 'msg_mean')
        plt.title('MEAN likes, views, messages per user (last 28 days)')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plt.xticks(rotation=90)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'mean_valus.png'
        plt.close()
        return plot_object
    
    @task
    def transform_vl(data):
        plt.figure(figsize=(12, 10))
        sns.set_theme(style="whitegrid")
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['views'], label = 'views')
        sns.lineplot(x = data['event_date'].dt.strftime('%m-%d'), y = data['likes'], label = 'likes')
        plt.title('Лайки и просмотры')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plt.xticks(rotation=90)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'lv.png'
        plt.close()
        return plot_object
    
    @task
    def transform_dau(data):
        plt.figure(figsize=(10, 8))
        sns.set_theme(style="whitegrid")
        sns.set_color_codes("pastel")
        sns.barplot(x=data['event_date'].dt.strftime('%m-%d'), y=data["feed_user"], color = 'b', label = 'feed users')
        sns.barplot(x=data['event_date'].dt.strftime('%m-%d'), y=data["msg_user"], color = 'r', label = 'messenger users')
        plt.title('DAU feed and messenger')
        plt.legend(loc="center right", frameon=True)
        plt.xlabel("Date")
        plt.ylabel("Users")
        plt.xticks(rotation=90)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU_feed_and_msg.png'
        plt.close()
        return plot_object
    
    @task
    def transform_new_users(data):
        plt.figure(figsize=(10, 8))
        sns.set_theme(style="whitegrid")
        sns.set_color_codes("pastel")
        sns.barplot(x=data['start_date'].dt.strftime('%m-%d'), y=data[data.grp == 'feed'].users, color = 'b', label = 'new feed users')
        sns.barplot(x=data['start_date'].dt.strftime('%m-%d'), y=data[data.grp == 'msg'].users, color = 'r', label = 'new messenger users')
        plt.title('New users feed and messenger')
        plt.legend(loc="center right", frameon=True)
        plt.xlabel("Date")
        plt.ylabel("Users")
        plt.xticks(rotation=90)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.namea = 'New_users_feed_and_msg.png'
        plt.close()
        return plot_object
    
    @task
    def transform_ngr(data):
        data.plot.bar(x = 'this_week', stacked = True, figsize=(14, 8), grid = True)
        plt.title('New, retained, gone users (last 60 days)')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plt.xticks(rotation=90)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'ngn.png'
        plt.close()
        return plot_object
    
    @task
    def transform_trf(data):
        data.plot.line(x = 'date', figsize=(14, 8), grid = True)
        plt.title('Users by source for feed and messenger')
        plt.legend()
        plt.xlabel("Date")
        plt.ylabel("")
        plt.xticks(rotation=90)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'ngn.png'
        plt.close()
        return plot_object
    
    @task
    def transform_top_posts(data):
        return 'Топ 10 постов за вчера:\n'+tabulate(data, headers=["Пост", "Лайки", "Показы", "Сумма"], tablefmt='plain', numalign="right")

    @task
    def transform_top_city(data):
        return 'Топ 10 городов за вчера:\n'+tabulate(data, headers=["Город", "Пользователи"], tablefmt='plain', numalign="right")
    
    @task
    def upload(mean_values_msg, dau_msg, new_users_msg, ngr_msg, top_post_msg, trf_msg, top_city_msg, vl_msg):
        send_msg(f"Report on {datetime.now().strftime('%m-%d')}") #hello msg 28-d
        send_graf(new_users_msg) #new users plot
        send_graf(dau_msg) #dau feed and msg'
        send_graf(ngr_msg) #new, gone, retained las 60 days
        send_graf(trf_msg) #источники трафика
        send_graf(mean_values_msg) #mean values lineplot()
        send_graf(vl_msg)
        
        send_msg(top_post_msg) #top 10 постов
        send_msg(top_city_msg)#top10 городов
        
    
    #extract
    first_data = extract_first_sql_str()
    new_users = extract_new_users()
    ngr = extract_new_gone_retained()
    top_posts = extract_top_posts()
    trf = extract_trafic()
    top_city =extract_top_city()
    
    #transform
    mean_values_msg = transform_mean_values(first_data)
    dau_msg  =transform_dau(first_data)
    vl_msg = transform_vl(first_data)
    new_users_msg = transform_new_users(new_users)
    ngr_msg = transform_ngr(ngr)
    top_post_msg = transform_top_posts(top_posts)
    trf_msg = transform_trf(trf)
    top_city_msg = transform_top_city(top_city)
    
    #load
    upload(mean_values_msg, dau_msg, new_users_msg, ngr_msg, top_post_msg, trf_msg, top_city_msg, vl_msg)
    
vmorozov_report_telegram_dag = vmorozov_report_telegram_dag()