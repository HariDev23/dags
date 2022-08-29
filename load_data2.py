from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
# import gspread
# from gspread_dataframe import get_as_dataframe, set_with_dataframe
# from oauth2client.service_account import ServiceAccountCredentials

from datetime import datetime

# https://docs.google.com/spreadsheets/d/11ikmmrGSMLHZgCIrRDa8aUUbgkK4A40xxZdloq-9yA8/edit?usp=sharing

def _read_data_gsheet():
    gsheetid ="11ikmmrGSMLHZgCIrRDa8aUUbgkK4A40xxZdloq-9yA8"
    sheet_name = "sample"
    gsheet_url = f"https://docs.google.com/spreadsheets/d/{gsheetid}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    # print(gsheet_url)
    df = pd.read_csv(gsheet_url)
    df.to_csv('/tmp/raw.csv')
    print('data loaded')


def _add_col():
    df = pd.read_csv('/tmp/raw.csv')
    df['discount'] = df['market_price'] - df['sale_price']
    df['description'] = "desc"
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.to_csv('/tmp/agg_data.csv', index=False, header = False)
    print('col added successfully')

def _store_data():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY done from stdin WITH DELIMITER as ','  ",
        filename = "/tmp/agg_data.csv"
    )

def _store_data1():
        #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_db').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.
    with open('/tmp/agg_data.csv', 'r') as f:
        next(f)
        curr.copy_from(f, 'example_table', sep=',')
        get_postgres_conn.commit()


with DAG('simple_ETL',
          start_date = datetime(2022,8,22),
          schedule_interval = '@daily',
          catchup = False
    ) as dag:

    create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',
    autocommit = True,
    sql='''
        CREATE TABLE IF NOT EXISTS done (
            index INTEGER ,
            product TEXT,
            category TEXT,
            sub_category TEXT,
            brand TEXT,
            sale_price INTEGER,
            market_price INTEGER,
            type TEXT,
            rating FLOAT,
            description TEXT,
            discount INTEGER
        );
        '''
    )


    read_data = PythonOperator(
        task_id = 'read_data',
        python_callable = _read_data_gsheet
    )

    add_discount_col = PythonOperator(
        task_id = 'add_col',
        python_callable = _add_col
    )

    store_data = PythonOperator(
        task_id = 'store_data',
        python_callable = _store_data
    )

create_table >> read_data >> add_discount_col >> store_data