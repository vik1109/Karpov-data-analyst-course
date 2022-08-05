#### ETL - pipeline, Airflow

| Название                                                     | Описание                                                     | Стек                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| [alert.py](https://github.com/vik1109/Karpov-data-analyst-course/blob/main/Airflow/alert.py) | Система Алетров для выявления аномалий в ключевых метриках и отпраки отчета в Telegram | Airflow, Python, Scipy, Pandas, Seaborn, Numpy, SQL , ClickHouse |
| [report.py](https://github.com/vik1109/Karpov-data-analyst-course/blob/main/Airflow/report.py) | Сбор, подготовка и отпрака отчетов по ленте сообщений в Telegram по рассписанию c использованием Airflow | Airflow, Python, Scipy, Pandas, Seaborn, Numpy, SQL , ClickHouse |
| [telega.py](https://github.com/vik1109/Karpov-data-analyst-course/blob/main/Airflow/telega.py) | Сбор, подготовка и отпрака отчетов по ленте и мессенджеру в Telegram по рассписанию c использованием Airflow | Airflow, Python, Scipy, Pandas, Seaborn, Numpy, telegram, SQL , ClickHouse |
| [v_morozov_dag_v.py](https://github.com/vik1109/Karpov-data-analyst-course/blob/main/Airflow/v_morozov_dag_v.py) | ETL - pipeline для выгрузки отчета в ClickHouse расписанию   | Airflow, Python, Scipy, Pandas, Seaborn, Numpy, telegram, SQL , ClickHouse |

