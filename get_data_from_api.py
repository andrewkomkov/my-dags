from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import requests
import json
import psycopg2
from pymongo import MongoClient
from bson.json_util import dumps
from urllib.parse import quote_plus as quote
from typing import List

postgres_conn = 'docker_host'

cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
hosts = [Variable.get("MONGO_DB_HOST")]
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")


class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 hosts: List[str],  # Список хостов для подключения
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


def get_info_from_mongo(collection_name,date):
    date = datetime.strptime(date, '%Y-%m-%d')
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, hosts, rs, db, db)
    dbs = mongo_connect.client()        
    # Объявляем параметры фильтрации
    filter = {'update_ts': {'$gt': date,'$lt':date + timedelta(days=1)}}
    # Объявляем параметры сортировки
    sort = [('update_ts', 1)]

    # Вычитываем документы из MongoDB с применением фильтра и сортировки
    l = list(dbs.get_collection(collection_name).find(filter=filter, sort=sort))
    to_db = []
    for obj in l:
        obj = json.loads(dumps(obj))
        m = {}
        m['object_id'] = obj['_id']['$oid']
        m['object_ts'] = obj['update_ts']['$date']
        obj.pop('_id',None)
        obj.pop('update_ts',None)
        m['object_value'] = json.dumps(obj)
        to_db.append(m)
    return to_db

conn = psycopg2.connect(
    host="host.docker.internal",
    database="de",
    user="jovyan",
    port = 5432,
    password="jovyan")

import logging

### Объявим нужные функции
def get_data_from_source(source_,offset_,from_ = '1970-01-01',to_ = '2099-01-01'):

    url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{source_}"

    querystring = {"sort_field":"date","sort_direction":"desc","limit":"50","offset":f"{offset_}","from":f"{from_}","to":f"{to_}"}

    payload = ""
    headers = {
        "X-Nickname": "adkomkov",
        "X-Cohort": "1",
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    logging.debug(querystring)


    logging.debug(response.text)

    return json.loads(response.text)

####
def get_info(source):
    to_db = []
    offset = 0
    l = get_data_from_source(f'{source}',offset)
    while len(l) != 0:
        print(len(l),'offset',offset)
        for obj in l:
            m = {}
            m['object_id'] = obj['_id']
            obj.pop('_id',None)
            m['object_value'] = json.dumps(obj)
            to_db.append(m)
        offset = offset + 50
        l = get_data_from_source(f'{source}',offset)
    return to_db

####
def get_info_deliv(source,date):
    date = datetime.strptime(date, '%Y-%m-%d')
    to_db = []
    offset = 0
    l = get_data_from_source(f'{source}',offset,f'{str(date)}',f'{str(date + timedelta(days=1))}')
    while len(l) != 0:
        print(len(l),'offset',offset)
        for obj in l:
            m = {}
            m['object_id'] = obj['delivery_id']
            m['object_ts'] = obj['delivery_ts']
            obj.pop('delivery_id',None)
            obj.pop('delivery_ts',None)
            m['object_value'] = json.dumps(obj)
            to_db.append(m)
        offset = offset + 50
        l = get_data_from_source(f'{source}',offset,f'{str(date)}',f'{str(date + timedelta(days=1))}')
        print(date)
    return to_db

#####
def api_to_db(source):
    # conn = dest.get_conn()
    cursor = conn.cursor()
    to_db =  get_info(source)
    for i in range(0,len(to_db)):
        cursor.execute(f'''INSERT INTO stg.api_{source} (object_id, object_value) VALUES('%s','%s')
        ON CONFLICT (object_id,object_value) DO NOTHING''' % (tuple(to_db[i].values())))
    conn.commit()

#####

def api_to_db_deliv(source,date):
    # conn = dest.get_conn()
    cursor = conn.cursor()
    to_db = get_info_deliv(source,date)


    for i in range(0,len(to_db)):
        cursor.execute('''INSERT INTO stg.api_deliveries (object_id, object_ts, object_value) VALUES('%s','%s','%s')
        ON CONFLICT (object_id,object_value) DO NOTHING''' % (tuple(to_db[i].values())))
    conn.commit()

######
def mongo_to_db(source,date):
    # conn = dest.get_conn()
    cursor = conn.cursor()
    to_db = get_info_from_mongo(source,date)
    
    for i in range(0,len(to_db)):
        cursor.execute(f'''INSERT INTO stg.mongo_{source} (object_id, object_ts, object_value) VALUES('%s','%s','%s')
        ON CONFLICT (object_id,object_value) DO NOTHING''' % (tuple(to_db[i].values())))
    conn.commit()


dag = DAG(
    dag_id='pjocert_3_data_load_docker_host',
    schedule_interval='0 6 * * *',
    start_date=datetime(2022, 6, 25),
    catchup=True,
    dagrun_timeout=timedelta(minutes=10)
)

get_restaurants = PythonOperator(task_id = 'get_restaurants',
                                python_callable = api_to_db,
                                op_kwargs = {"source": 'restaurants'},
                                dag=dag)

get_couriers = PythonOperator(task_id = 'get_couriers',
                                python_callable = api_to_db,
                                op_kwargs = {"source": 'couriers'},
                                dag=dag)

get_deliveries = PythonOperator(task_id = 'api_to_db_deliv',
                                python_callable = api_to_db_deliv,
                                op_kwargs = {"source": 'deliveries',"date":'{{ds}}'},
                                dag=dag)
get_orders = PythonOperator(task_id = 'get_orders',
                                python_callable = mongo_to_db,
                                op_kwargs = {"source": 'orders',"date":'{{ds}}'},
                                dag=dag)
get_users = PythonOperator(task_id = 'get_users',
                                python_callable = mongo_to_db,
                                op_kwargs = {"source": 'users',"date":'{{ds}}'},
                                dag=dag)



filling_dds_addresses = PostgresOperator(
        task_id='filling_dds_addresses',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_addresses.sql",
        dag = dag
    )

filling_dds_couriers = PostgresOperator(
        task_id='filling_dds_couriers',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_couriers.sql",
        dag = dag
    )

filling_dds_restarauns = PostgresOperator(
        task_id='filling_dds_restarauns',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_restarauns.sql",
        dag = dag
    )

filling_dds_timestamps = PostgresOperator(
        task_id='filling_dds_timestamps',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_timestamps.sql",
        dag = dag
    )

filling_dds_users = PostgresOperator(
        task_id='filling_dds_users',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_users.sql",
        dag = dag
    )


filling_dds_orders = PostgresOperator(
        task_id='filling_dds_orders',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_orders.sql",
        dag = dag
    )


filling_dds_deliveres = PostgresOperator(
        task_id='filling_dds_deliveres',
        postgres_conn_id=postgres_conn,
        sql="sql/filling_dds_deliveres.sql",
        dag = dag
    )

# filling_dds_addresses = PostgresOperator(
#         task_id='filling_dds_addresses',
#         postgres_conn_id=postgres_conn,
#         sql="sql/filling_dds_addresses.sql",
#         dag = dag
#     )

[get_restaurants >> get_couriers] >> get_deliveries >> get_orders >> get_users >> filling_dds_addresses >> filling_dds_couriers >> filling_dds_restarauns >> filling_dds_timestamps >> filling_dds_users >> filling_dds_orders >> filling_dds_deliveres