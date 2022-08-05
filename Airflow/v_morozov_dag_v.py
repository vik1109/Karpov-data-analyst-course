#импорт библиотек
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse

#импорт библиотек airflow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#строка для соединения с CH db = simulator
connection_simulator = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'password',
    'user': 'student',
    'database': 'simulator'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'password',
    'user': 'student-rw',
    'database': 'test'
}

feed_query = """SELECT user_id,
                       countIf(action = 'view') AS views,
                       countIf(action = 'like') AS likes,
                       os,
                       gender,
                       age, 
                       toDate(time) AS event_date
                FROM simulator_20220620.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, os, gender, age, event_date"""

msg_query ="""SELECT user_id,
       messages_received,
       users_received,
       messages_sent,
       users_sent,
       age,
       os,
       gender,
       yesterday() AS event_date
FROM
  (SELECT if(t1.user_id = 0, t2.reciever_id, t1.user_id) AS user_id,
          t1.recieved_msg AS messages_received,
          t1.reciever_count AS users_received,
          t2.messages_send AS messages_sent,
          t2.send_count AS users_sent
   FROM 
           (SELECT user_id,
                   COUNT(reciever_id) AS recieved_msg,
                   COUNT(DISTINCT reciever_id) AS reciever_count
            FROM simulator_20220620.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id) AS t1
         FULL OUTER JOIN
           (SELECT reciever_id,
                   COUNT(*) AS messages_send,
                   uniqExact(user_id) AS send_count
            FROM simulator_20220620.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY reciever_id) AS t2 ON t1.user_id = t2.reciever_id) AS u
   LEFT JOIN
     (SELECT DISTINCT user_id,
                      age,
                      gender,
                      os
      FROM simulator_20220620.message_actions
      UNION DISTINCT SELECT DISTINCT user_id,
                                     age,
                                     gender,
                                     os
      FROM simulator_20220620.feed_actions)AS t3 ON u.user_id = t3.user_id
"""

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
def vmorozov_dag():
        
    @task
    def extract_feed():
        df_feed = pandahouse.read_clickhouse(feed_query, connection=connection_simulator)
        return df_feed
    
    @task
    def extract_msg():
        df_msg = pandahouse.read_clickhouse(msg_query, connection=connection_simulator)
        return df_msg
    
    @task
    def transform_merge(df_feed, df_msg):
        df_merge = df_feed.merge(df_msg, how = 'outer', on = ['user_id', 'os', 'gender', 'age', 'event_date'])
        df_merge = df_merge.fillna(0)
        return df_merge
    
    @task
    def transform_os(df_merge):
        df_merge['metric'] = 'os'
        df_os = df_merge.groupby(by = ['event_date', 'metric', 'os']).agg(
            {'likes': 'sum',
             'views':'sum',
             'messages_received':'sum',
             'messages_sent':'sum',
             'users_received':'sum',
             'users_sent':'sum'}).reset_index()
        df_os.rename(columns = {'os' : 'submetric'}, inplace = True)
        return df_os
    
    @task
    def transform_gender(df_merge):
        df_merge['metric'] = 'gender'
        df_merge['gender']=df_merge['gender'].apply(lambda x: 'Male' if x == 0 else 'Female')
        df_gender = df_merge.groupby(by = ['event_date', 'metric', 'gender']).agg(
            {'likes': 'sum',
             'views':'sum',
             'messages_received':'sum',
             'messages_sent':'sum',
             'users_received':'sum',
             'users_sent':'sum'}).reset_index()
        df_gender.rename(columns = {'gender' : 'submetric'}, inplace = True)
        return df_gender
    
    @task
    def transform_age(df_merge):
        df_merge['metric'] = 'age'
        df_merge['age_labels'] = pd.cut(df_merge['age'],
                                        bins = [0, 18, 25, 35, 50, 200], 
                                        labels = ['0-18', '18-25', '25-35', '35-50', '50+']
                                       )
        df_age = df_merge.groupby(by = ['event_date', 'metric', 'age_labels']).agg(
            {'likes': 'sum',
             'views':'sum',
             'messages_received':'sum',
             'messages_sent':'sum',
             'users_received':'sum',
             'users_sent':'sum'}).reset_index()
        df_age.rename(columns = {'age_labels' : 'submetric'}, inplace = True)
        return df_age
    
    @task
    def transform_df(os, gender, age):
        df_cube = pd.concat([os, gender, age])
        df_cube = df_cube.astype({'likes': 'int32', 
                                  'views': 'int32',
                                  'messages_received': 'int32',
                                  'messages_sent': 'int32',
                                  'users_received': 'int32',
                                  'users_sent': 'int32',})
        return df_cube
    
    @task
    def upload(df_cube):
        pandahouse.to_clickhouse(df_cube, table = 'vmorozov', index=False, connection=connection_test)
    
    #extract
    df_feed = extract_feed()
    df_msg = extract_msg()
    
    #transform
    df_merged = transform_merge(df_feed, df_msg)
    df_by_os = transform_os(df_merged)
    df_by_gender = transform_gender(df_merged)
    df_by_age = transform_age(df_merged)
    df_cube = transform_df(df_by_os, df_by_gender, df_by_age)
    
    #load
    upload(df_cube)
    
vmorozov_dag = vmorozov_dag()