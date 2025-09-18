import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task, task_group
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
import logging
from pokemon_data_ingester import PokemonDataIngester


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


POKEMON_API_URL = os.getenv("POKEMON_API_URL")
NUM_POKEMON_PER_RUN = os.getenv("NUM_POKEMON_PER_RUN")
MAX_POKEMON_ID = os.getenv("MAX_POKEMON_ID")
VARIABLE_NAME = os.getenv("VARIABLE_NAME")
NEXT_DAG_ID = "bronze_to_silver_dag"
MINIO_BUCKET_POKEMON = os.getenv("MINIO_BUCKET_POKEMON")
ABILITY_API_URL = os.getenv("ABILITY_API_URL")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pokemon_ingestion_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
def pokemon_ingest_dag():

    @task
    def ingest_pokemon():
        ingester = PokemonDataIngester()
        ingester.ingest_pokemon_base()

    @task
    def ingest_abilities():
        ingester = PokemonDataIngester()
        ingester.ingest_pokemon_abilities()

    @task_group(group_id="ingest_pokemon_data")
    def ingest_pokemon_data():
        ingest_pokemon_base_task = ingest_pokemon()
        ingest_pokemon_abilities_task = ingest_abilities()

        [ingest_pokemon_base_task, ingest_pokemon_abilities_task]

        return ingest_pokemon_base_task, ingest_pokemon_abilities_task


    start = DummyOperator(task_id="start_dag")

    pokemon_ingestion_group = ingest_pokemon_data()

    # trigger_next = TriggerDagRunOperator(
    #     task_id="trigger_next_dag",
    #     trigger_rule="all_success",
    #     trigger_dag_id=NEXT_DAG_ID,
    #     wait_for_completion=False
    # )

    end = DummyOperator(
        task_id="end_dag"
    )

    start >> pokemon_ingestion_group >> end

dag = pokemon_ingest_dag()