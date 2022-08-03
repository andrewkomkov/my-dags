from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

postgres_conn = 'docker_host'

initial_dag = DAG(
    dag_id='initial_dag',
    schedule_interval= None,
    start_date=datetime(2022, 6, 25),
    catchup=False
)

create_shemas = PostgresOperator(
        task_id='create_shemas',
        postgres_conn_id=postgres_conn,
        sql="sql/ddl_schemas.sql",
        dag = initial_dag
    )

create_func = PostgresOperator(
        task_id='create_func',
        postgres_conn_id=postgres_conn,
        sql="sql/ddl_func.sql",
        dag = initial_dag
    )

create_stg = PostgresOperator(
        task_id='create_stg',
        postgres_conn_id=postgres_conn,
        sql="sql/ddl_stg.sql",
        dag = initial_dag
    )

create_dds = PostgresOperator(
        task_id='create_dds',
        postgres_conn_id=postgres_conn,
        sql="sql/ddl_dds.sql",
        dag = initial_dag
    )


create_cdm = PostgresOperator(
        task_id='create_cdm',
        postgres_conn_id=postgres_conn,
        sql="sql/ddl_cdm.sql",
        dag = initial_dag
    )

fill_coefs = PostgresOperator(
        task_id='fill_coefs',
        postgres_conn_id=postgres_conn,
        sql="sql/insert_couriers_coefs.sql",
        dag = initial_dag
    )

create_shemas >> create_func >> create_stg >> create_dds >> create_cdm >> fill_coefs