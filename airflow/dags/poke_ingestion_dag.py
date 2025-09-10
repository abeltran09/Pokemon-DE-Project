import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import requests
from airflow.models import Variable
from airflow.api.client.local_client import Client
from minio_client import store_to_minio
from dotenv import load_dotenv
import time


load_dotenv()


POKEMON_API_URL = os.getenv("POKEMON_API_URL")
NUM_POKEMON_PER_RUN = os.getenv("NUM_POKEMON_PER_RUN")
MAX_POKEMON_ID = os.getenv("MAX_POKEMON_ID")
VARIABLE_NAME = os.getenv("VARIABLE_NAME")
NEXT_DAG_ID = "bronze_to_silver_dag"

NUM_POKEMON_PER_RUN = int(NUM_POKEMON_PER_RUN)
MAX_POKEMON_ID = int(MAX_POKEMON_ID)



def get_last_pokemon_id():
    """Get the last ingested Pokémon ID from Airflow Variable."""
    return int(Variable.get(VARIABLE_NAME, default_var=1))

def set_last_pokemon_id(pokemon_id):
    """Update Airflow Variable with last ingested Pokémon ID."""
    Variable.set(VARIABLE_NAME, pokemon_id)


def self_pause():
    client = Client(None)
    client.pause_dag(dag_id="pokemon_ingestion_dag")


def ingest_pokemon_batch():
    start_id = get_last_pokemon_id()
    end_id = min(start_id + NUM_POKEMON_PER_RUN - 1, MAX_POKEMON_ID)

    for pokemon_id in range(start_id, end_id + 1):
        try:
            response = requests.get(POKEMON_API_URL.format(pokemon_id))
            response.raise_for_status()
            pokemon_data = response.json()
            store_to_minio(pokemon_data, pokemon_id)
            time.sleep(0.1)
        except Exception as e:
            print(f"Failed to fetch Pokémon {pokemon_id}: {e}")

    # Update file for next run; do not exceed 1025
    next_id = end_id + 1 if end_id < MAX_POKEMON_ID else MAX_POKEMON_ID + 1
    set_last_pokemon_id(next_id)


def branch_on_completion(**kwargs):
    """Decide whether to trigger next DAG or end."""
    last_id = get_last_pokemon_id()
    if last_id > MAX_POKEMON_ID:
        return "trigger_next_dag"
    else:
        return "end_dag"


# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),  # starts yesterday
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="pokemon_ingestion_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

start = DummyOperator(
    task_id="start_dag",
    dag=dag
)

ingest_task = PythonOperator(
    task_id="ingest_pokemon_task",
    python_callable=ingest_pokemon_batch,
    dag=dag
)

branch_task = BranchPythonOperator(
    task_id="branch_on_completion",
    python_callable=branch_on_completion,
    provide_context=True,
    dag=dag
)

trigger_next = TriggerDagRunOperator(
    task_id="trigger_next_dag",
    trigger_dag_id=NEXT_DAG_ID,
    wait_for_completion=False,
    dag=dag
)

end = DummyOperator(
    task_id="end_dag"
)

start >> ingest_task >> branch_task
branch_task >> trigger_next >> end
branch_task >> end