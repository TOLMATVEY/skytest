# Импорт необходимых библиотек
import psycopg2 as ps
import os
import re
import pandas as pd
from datetime import datetime
import logging
import requests
import psycopg2.extras
import my_decorator1

# настройка логирования
if not os.path.isdir("logs"):
    os.mkdir("logs")
logging.basicConfig(level=logging.INFO, filename=f"./logs/{datetime.now().date()}.log",
                    format="%(asctime)s %(levelname)s %(message)s")
BOT_TOKEN = os.getenv('bot_token')

# список таблиц стейджа
STAGE_TABLES = ["skyeng.stg_stream", "skyeng.stg_course",
                "skyeng.stg_stream_module", "skyeng.stg_stream_module_lesson"
                ]

# список таблиц витрин данных
DATA_MARTS = ['frauds.sql']

# скрипты загрузки из стейдж в таргет
SQL_SCRIPTS_TO_DWH_SCD2 = ['stg_to_hub.sql', 'stg_to_satellite.sql',
                           'stg_to_link.sql', 'update_satellite.sql',
                           'delete_satellite.sql, update_meta_inf.sql'
                           ]

# Очистка таблиц
@my_decorator1.retry(max_tries = 5, delay_seconds = 5)
def clear_tables(cursor, tables: list):
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            logging.info(f"Таблица очищена {table}")
        except Exception as e:
            log_message = f"Ошибка очистки таблицы {table}: {e}"
            processing_error_message(log_message)

# Выполение скриптов в папке
@my_decorator1.retry(max_tries = 5, delay_seconds = 5)
def execute_sql_scripts(cursor, scripts: list):
    for script in scripts:
        try:
            with open(f'./skyeng_sqlscripts/{script}', 'r') as sql_file:
                cursor.execute(sql_file.read())
                logging.info(f"Скрипт выполнен {script}")
        except Exception as e:
            log_message = f"Ошибка выполнения скрипта {script}: {e}"
            processing_error_message(log_message)

# Информация телеграм боту
@my_decorator1.retry(max_tries = 5, delay_seconds = 5)
def processing_error_message(message: str):
    logging.error(message)
    requests.post(url=f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
                  data={'chat_id': 397804346, 'text': message}).json()

logging.info(f"Старт скрипта")


try:
    # Создаем подключение к источнику и хранилищу
    connect1 = ps.connect(database = "mydb",
                          host =     "mydb-skyeng-chronosavant.ru",
                          user =     "tolmat",
                          password = "sarumanthewhite",
                          port =     "5432")

    connect2 = ps.connect(database = "sourcedb",
                          host =     "skyeng-db.chronosavant.ru",
                          user =     "skyeng_etl",
                          password = "skyeng_etl_pass",
                          port =     "5432")
    
    # Создаем курсоры
    cursor1 = connect1.cursor()
    cursor2 = connect2.cursor()
    
    # Отключаем автокоммит
    connect1.autocommit = False
    
    logging.info(f"Очистка таблиц в Stage:")
    clear_tables(cursor1, STAGE_TABLES)

    logging.info(f"Загрузка данных из источника в STAGE:")
    logging.info(f"Заполнение skyeng.stg_course")
    cursor2.execute(''' SELECT id, title, created_at, update_dt, deleted_at, 
                               icon_url, is_auto_course_enroll, is_demo_enroll  
                        FROM info.course 
                    ''')
    
    cursor1.execute(''' select max_update_dt  
                        from skyeng.meta_inf 
                        where schema_name='skyeng' 
                        and table_name='course' 
                    ''')
                    
    records = cursor2.fetchall()
    max_date = cursor1.fetchall()
    
    # Копаясь в документации, наткнулся на интересную функцию execute_values.
    # Если по простому, функция генерирует огромный список параметров в запрос.
    # При этом, функция потребляла достаточное количество памяти нежели ее собратья
    # executemany, batch и прочие, происходило это из-за того, что что наши значения 
    # хранились в краткосрочной памяти перед использованием их psycopg.
    # Выход? Будем "выкрикивать" значения из итератора, ведь значение в 
    # памяти не сохранится! (конечно, перед записью, сравним данные с метой)
    # И как результат, загрузку удалось ускорить в сотни раз (в тестовой среде), память наш
    # запрос почти не кушает, и вообще, все здорово.
    # Конечно, здесь можно ипользовать дополнительный параметр size_page,
    # он позволит еще больше ускорить загрузку, но это избыточно для этих данных. 
    psycopg2.extras.execute_values(
        cursor1, '''insert into skyeng.stg_course( id, title, created_at, update_dt, deleted_at, 
                                                   icon_url, is_auto_course_enroll, is_demo_enroll ) VALUES %s 
                 ''', [(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], )
                      for i in records if i[3] >= max_date[0][0]])
    
    logging.info(f"Заполнение skyeng.stg_stream")            
    cursor2.execute(''' SELECT id, course_id, start_dt, end_dt, created_at, update_dt, 
                               deleted_at, is_open, course_name, homework_deadline_days  
                        FROM info.stream
                    ''')
                    
    cursor1.execute(''' select max_update_dt  
                        from skyeng.meta_inf
                        where schema_name='skyeng' 
                        and table_name='stream' 
                    ''')
    
    records = cursor2.fetchall()
    max_date = cursor1.fetchall()
    
    psycopg2.extras.execute_values(
        cursor1, '''insert into skyeng.stg_stream( id, course_id, start_dt, end_dt, created_at, 
                                                   update_dt, deleted_at, is_open, course_name, 
                                                   homework_deadline_days ) VALUES %s 
                 ''', [(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], )
                      for i in records if i[5] >= max_date[0][0]])
                    
    logging.info(f"Заполнение skyeng.stg_stream_module")                  
    cursor2.execute(''' SELECT  id, stream_id, title, order_in_stream, 
                                created_at, update_dt, deleted_at  
                        FROM info.stream_module
                    ''')
                    
    cursor1.execute(''' select max_update_dt  
                        from skyeng.meta_inf
                        where schema_name='skyeng' 
                        and table_name='stream_module' 
                    ''')
    
    records = cursor2.fetchall()
    max_date = cursor1.fetchall()
    
    psycopg2.extras.execute_values(
        cursor1, '''insert into skyeng.stg_stream_module( id, stream_id, title, order_in_stream, 
                                                          created_at, update_dt, deleted_at  ) VALUES %s 
                 ''', [(i[0], i[1], i[2], i[3], i[4], i[5], i[6], )
                       for i in records if i[5] >= max_date[0][0]])
                    
    logging.info(f"Заполнение skyeng.stg_stream_module_lesson")                                  
    cursor2.execute(''' SELECT  id, title, description, start_at, end_at, homework_url, 
                                teacher_id, stream_module_id, deleted_at, 
                                online_lesson_join_url, online_lesson_recording_url 
                        FROM info.stream_module_lesson
                    ''')
                    
    cursor1.execute(''' select max_update_dt  
                        from skyeng.meta_inf
                        where schema_name='skyeng' 
                        and table_name='stream_module_lesson' 
                    ''')
    
    records = cursor2.fetchall()
    max_date = cursor1.fetchall()
    
    psycopg2.extras.execute_values(
        cursor1, '''insert into skyeng.stg_stream_module_lesson( id, title, description, start_at, 
                                                                 end_at, homework_url, teacher_id, 
                                                                 stream_module_id, deleted_at, 
                                                                 online_lesson_join_url, 
                                                                 online_lesson_recording_url  ) VALUES %s 
                 ''', [(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], )
                      for i in records if i[3] >= max_date[0][0]])
                    
    logging.info(f"Загрузка данных из STAGE в DETAIL:")
    execute_sql_scripts(cursor1, SQL_SCRIPTS_TO_DWH_SCD2)
     
    logging.info(f"Загрузка данных в витрину:")
    execute_sql_scripts(cursor1, DATA_MARTS)

except (Exception, psycopg2.Error) as error:
    log_message = f"Ошибка при работе с PostgreSQL: {error}"
    processing_error_message(log_message)
# Когда все наши скрипты отработали свое, соединение с базой закрывается, 
# Сохраняются все измения в базе.
finally:
    if connect1 or connect2:
        connect1.close()
        connect2.close()
        cursor1.close()
        cursor2.close()
        logging.info(f"Соединение с PostgreSQL закрыто") 
        
logging.info(f"Завершение скрипта")               