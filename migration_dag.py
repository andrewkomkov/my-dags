from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator



src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
dest = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')


def migrate_ranks(src,dest):
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    cursor.execute("SELECT * FROM public.ranks")
    try:
        dest.insert_rows(table="stg.bonussystem_ranks", rows=cursor)
    except:
        pass

def migrate_users(src,dest):
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    cursor.execute("SELECT * FROM public.users")
    try:
        dest.insert_rows(table="stg.bonussystem_users", rows=cursor)
    except:
        pass


def migrate_events(src,dest):
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()

    dest_cursor.execute("SELECT MAX(id) FROM stg.bonussystem_events;")
    id_ = dest_cursor.fetchone()[0]
    if id_ is None:
        id_ = 0
    cursor.execute(f"SELECT * FROM public.outbox WHERE id > {id_}")

    dest.insert_rows(table="stg.bonussystem_events", rows=cursor)
    dest_cursor.execute(f"Insert into stg.srv_wf_settings (workflow_key) VALUES ({id_})")

dag = DAG(
    dag_id='migration',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 7, 22),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
)


    # dest_cursor.execute("SELECT MAX(product_id) FROM products;")
    # product_id = dest_cursor.fetchone()[0]
    # if product_id is None:
    #     product_id = 0
    
    # dest_cursor.execute("SELECT MAX(order_id) FROM orders;")
    # order_id = dest_cursor.fetchone()[0]
    # if order_id is None:
    #     order_id = 0
    # cursor.execute("SELECT * FROM orders WHERE order_id > %s", [order_id])
    # dest.insert_rows(table="orders", rows=cursor)

migrate_ranks = PythonOperator(task_id = 'migrate_ranks',
                                python_callable = migrate_ranks,
                                op_kwargs = {"src": src, "dest":dest},
                                dag=dag)
            
migrate_users = PythonOperator(task_id = 'migrate_users',
                                python_callable = migrate_users,
                                op_kwargs = {"src": src, "dest":dest},
                                dag=dag)
migrate_events = PythonOperator(task_id = 'migrate_events',
                                python_callable = migrate_events,
                                op_kwargs = {"src": src, "dest":dest},
                                dag=dag)

migrate_events >> migrate_users >> migrate_ranks