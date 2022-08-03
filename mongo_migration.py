from airflow.models import Variable
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import json

cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
hosts = [Variable.get("MONGO_DB_HOST")]
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")




dag = DAG(
    dag_id='mongo_migration',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 22),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
)

from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient

# Заводим класс для подключения к MongoDB
class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 host: List[str],  # Список хостов для подключения
                 rs: str,  # replica set.
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    # Формируем строку подключения к MongoDB
    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=','.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)

    # Создаём клиент к БД
    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]


# Создаём клиент к БД


def get_info_from_mongo(ti,collection_name):
    engine = create_engine('postgresql://jovyan:jovyan@localhost:5432/de')
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, hosts, rs, db, db)
    dbs = mongo_connect.client()        
    # Объявляем параметры фильтрации
    filter = {'update_ts': {'$lt': datetime.utcnow()}}
    # Объявляем параметры сортировки
    sort = [('update_ts', 1)]
    # collection_name = 'users'
    # Вычитываем документы из MongoDB с применением фильтра и сортировки
    docs = list(dbs.get_collection(collection_name).find(filter=filter, sort=sort))
    # ti.xcom_push(key='docs', value=docs)
    df = pd.DataFrame.from_dict(docs)
    df.to_csv(f'/lessons/dags/{collection_name}.csv')
    pf = pd.DataFrame()
    pf['object_id'] = df['_id'].astype('str')
    pf['update_ts'] = df['update_ts']
    df.drop(['_id','update_ts'],inplace = True,axis=1)
    pf['object_value'] = df.apply(lambda x: str(x.to_dict()).replace("'",'"').replace('ObjectId("','"').replace('Timestamp("','"').replace('")','"'),axis=1)
    engine.execute(f'DELETE  FROM stg.ordersystem_{collection_name}')
    pf.to_sql(name=f'ordersystem_{collection_name}',con = engine,schema = 'stg',if_exists = 'append',index=False)


# def send_data_to_postgres(ti):
#     engine = create_engine('postgresql://jovyan:jovyan@localhost:5432/de')
#     docs = ti.xcom_pull(key='docs')
#     df = pd.DataFrame.form_dict(docs)
#     df.to_csv('/lessons/dags/t.csv')

get_info_from_mongo_task_users =  PythonOperator(
            task_id='get_info_from_mongo_task_users',
            python_callable=get_info_from_mongo,
            op_kwargs = {'collection_name' : 'users'},
            dag = dag
        )

get_info_from_mongo_task_restaurants =  PythonOperator(
            task_id='get_info_from_mongo_task_restaurants',
            python_callable=get_info_from_mongo,
            op_kwargs = {'collection_name' : 'restaurants'},
            dag = dag
        )

get_info_from_mongo_task_orders =  PythonOperator(
            task_id='get_info_from_mongo_task_orders',
            python_callable=get_info_from_mongo,
            op_kwargs = {'collection_name' : 'orders'},
            dag = dag
        )

get_info_from_mongo_task_users  >> get_info_from_mongo_task_orders >> get_info_from_mongo_task_restaurants