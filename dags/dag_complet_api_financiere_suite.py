import logging
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd
import random
import requests
from airflow.decorators import dag, task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}localization=fr"

N = 100

@dag(dag_id="dag_complet_api_financiere_suite", schedule="@once", start_date=datetime(2023, 3, 25), catchup=False)
def taskflow():
    @task(task_id="extract", retries=3, retry_delay=timedelta(minutes=2))
    def extract_bitcoin_price(task_instance) -> Dict[str, float]:
        execution_date = task_instance.execution_date
        dates = []
        prices = []
        formatted_date = execution_date.strftime("%d-%m-%Y")
        response = requests.get(API.format(formatted_date)).json()["market_data"]
        initial_price = response["current_price"]["usd"]

        for n_day_before in range(0, N):
            date = execution_date - timedelta(days=n_day_before)
            formatted_date = date.strftime("%d-%m-%Y")
            random_variation = round(random.uniform(-1000, 1000), 2)
            dates.append(date)
            prices.append(initial_price + random_variation)
        
        return {"dates": dates, "prices": prices}

    @task(multiple_outputs=True)
    def process_data(extraction: Dict[str, float]) -> Dict[str, float]:
        logging.info(extraction)
        prices = extraction["prices"][::-1]
        print(prices)
        deltas = np.diff(prices)
        print(deltas)
        ups = [delta for delta in deltas if delta > 0]
        downs = [-delta for delta in deltas if delta < 0]
        up_avg = sum(ups) / len(ups)
        print(up_avg)
        down_avg = sum(downs) / len(downs)
        print(down_avg)
        rsi = 100 - 100/(1 + up_avg/down_avg)

        print(len(extraction["dates"]))
        print(len(prices))

        return {
            "dates": extraction["dates"],
            "prices": prices,
            "rsi": rsi,
        }

    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS bitcoin_data (
                date DATE PRIMARY KEY,
                price FLOAT,
                rsi FLOAT
            );
        """
        hook.run(create_table_sql)

    @task
    def insert_data(data):
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        rows = [(date, price, data['rsi']) for date, price in zip(data['dates'], data['prices'])]
        insert_sql = """
            INSERT INTO bitcoin_data (date, price, rsi)
            VALUES (%s, %s, %s)
            ON CONFLICT (date)
            DO NOTHING;
        """
        hook.insert_rows(
            table="bitcoin_data",
            rows=rows,
            target_fields=["date", "price", "rsi"],
            replace=True,
            replace_index="date",
        )

    @task
    def load_data_from_db():
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        df = hook.get_pandas_df(sql="SELECT * FROM bitcoin_data ORDER BY date")
        return df

    def branch_task(df):
        if len(df) > 100:
            return "calculate_weekly_average"
        return "not_enough_data"

    create_table_task = create_table()
    extracted_data_task = process_data(extract_bitcoin_price())
    insert_data_task = insert_data(extracted_data_task)
    load_data_task = load_data_from_db()

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_task,
        op_args=[load_data_task]
    )

    calculate_weekly_average = BashOperator(
        task_id="calculate_weekly_average",
        bash_command="echo 'Calculating weekly prices';",
    )

    not_enough_data = BashOperator(
        task_id="not_enough_data",
        bash_command="echo 'Not enough data';"
    )

    [create_table_task, extracted_data_task] >> insert_data_task >> load_data_task >> branching
    branching >> calculate_weekly_average
    branching >> not_enough_data

taskflow()