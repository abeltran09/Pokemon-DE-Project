from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from minio_client import store_to_minio, get_s3_client
import uuid
import pandas as pd
import json
from datetime import datetime
import io
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": "False",
    "start_date": datetime.now(),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="bronze_to_silver",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
def bronze_to_silver_dag():

    @task_group(group_id="setup_elt_tables")
    def setup_elt_tables():
        create_staging_schema = PostgresOperator(
            task_id="create_schemas",
            postgres_conn_id='poke-postgres',
            sql="""CREATE SCHEMA IF NOT EXISTS staging;"""
        )

        table_tasks = []

        for table in ["stg_pokemon_base", "stg_pokemon_abilities", "stg_abilities"]:
            create_table = PostgresOperator(
                task_id=f"create_{table}",
                postgres_conn_id="poke-postgres",
                sql=f"ddl/ddl_{table}.sql"
            )
            table_tasks.append(create_table)

        create_staging_schema >> table_tasks

    @task_group(group_id="bronze_to_staging")
    def bronze_to_staging():

        @task
        def load_pokemon_bronze_to_staging():
            """Load Pokemon data from bronze Parquet files to PostgreSQL staging table"""
            s3_client = get_s3_client()
            postgres_hook = PostgresHook(postgres_conn_id='poke-postgres')

            today = datetime.now().strftime('%Y-%m-%d')
            bronze_prefix = f"bronze/pokemon/date={today}/"

            logging.info(f"Loading Pokemon bronze data from prefix: {bronze_prefix}")

            all_objects = []
            continuation_token = None
            page_count = 0

            while True:
                page_count += 1
                logging.info(f"Fetching page {page_count} from MinIO bronze layer...")

                list_params = {
                    'Bucket': 'pokemon-lake',
                    'Prefix': bronze_prefix
                }
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                response = s3_client.list_objects_v2(**list_params)
                page_objects = response.get('Contents', [])

                parquet_objects = [obj for obj in page_objects if obj['Key'].endswith('.parquet')]
                all_objects.extend(parquet_objects)

                logging.info(
                    f"Page {page_count}: Found {len(parquet_objects)} Parquet files (Total: {len(all_objects)})")

                if not response.get('IsTruncated', False):
                    break

                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    break

            logging.info(f"Found {len(all_objects)} bronze Parquet files to process")

            if not all_objects:
                logging.warning("No bronze Parquet files found for today")
                return None

            batch_id = str(uuid.uuid4())[:8]
            all_records = []

            for i, obj in enumerate(all_objects, 1):
                file_key = obj['Key']
                logging.info(f"Processing bronze file {i}/{len(all_objects)}: {file_key}")

                try:
                    response = s3_client.get_object(Bucket='pokemon-lake', Key=file_key)
                    parquet_data = response['Body'].read()

                    df = pd.read_parquet(io.BytesIO(parquet_data))

                    logging.info(f"Loaded {len(df)} records from {file_key}")

                    for _, row in df.iterrows():
                        record = (
                            row.get('pokemon_id'),
                            row.get('name'),
                            row.get('height'),
                            row.get('weight'),
                            row.get('base_experience'),
                            row.get('type_1'),
                            row.get('type_2'),
                            row.get('hp'),
                            row.get('attack'),
                            row.get('defense'),
                            row.get('special_attack'),
                            row.get('special_defense'),
                            row.get('speed'),
                            row.get('sprite_url'),
                            row.get('batch_id'),
                            row.get('ingested_at'),
                            row.get('source_url'),
                            row.get('ingestion_date'),
                            row.get('bronze_processed_at'),
                            row.get('source_file'),
                            batch_id,
                            datetime.now().isoformat()
                        )
                        all_records.append(record)

                except Exception as e:
                    logging.error(f"Error processing bronze file {file_key}: {e}")
                    continue

            if not all_records:
                raise Exception("No valid records to load to staging")

            logging.info(f"Total records to insert into staging: {len(all_records)}")

            postgres_hook.run(
                "DELETE FROM staging.stg_pokemon_base WHERE DATE(loaded_at) = CURRENT_DATE"
            )

            target_fields = [
                'pokemon_id', 'name', 'height', 'weight', 'base_experience',
                'type_1', 'type_2', 'hp', 'attack', 'defense', 'special_attack',
                'special_defense', 'speed', 'sprite_url', 'bronze_batch_id',
                'ingested_at', 'source_url', 'ingestion_date', 'bronze_processed_at',
                'source_file', 'staging_batch_id', 'loaded_at'
            ]

            postgres_hook.insert_rows(
                table='staging.stg_pokemon_base',
                rows=all_records,
                target_fields=target_fields,
                commit_every=1000,
                fast_executemany=True
            )

            final_count = postgres_hook.get_first(
                f"SELECT COUNT(*) FROM staging.stg_pokemon_base WHERE staging_batch_id = '{batch_id}'"
            )[0]

            logging.info(f"=== POKEMON BRONZE TO STAGING COMPLETE ===")
            logging.info(f"Records inserted: {final_count}")
            logging.info(f"Staging batch ID: {batch_id}")

            return {
                'staging_batch_id': batch_id,
                'records_loaded': final_count,
                'bronze_files_processed': len(all_objects)
            }

        @task
        def load_abilities_bronze_to_staging():
            """Load Abilities data from bronze Parquet files to PostgreSQL staging table"""
            s3_client = get_s3_client()
            postgres_hook = PostgresHook(postgres_conn_id='poke-postgres')

            today = datetime.now().strftime('%Y-%m-%d')

            results = {}

            abilities_prefix = f"bronze/abilities/date={today}/"
            logging.info(f"Loading abilities bronze data from prefix: {abilities_prefix}")

            abilities_objects = []
            continuation_token = None

            while True:
                list_params = {
                    'Bucket': 'pokemon-lake',
                    'Prefix': abilities_prefix
                }
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                response = s3_client.list_objects_v2(**list_params)
                page_objects = response.get('Contents', [])
                parquet_objects = [obj for obj in page_objects if obj['Key'].endswith('.parquet')]
                abilities_objects.extend(parquet_objects)

                if not response.get('IsTruncated', False):
                    break
                continuation_token = response.get('NextContinuationToken')

            if abilities_objects:
                batch_id = str(uuid.uuid4())[:8]
                all_ability_records = []

                for obj in abilities_objects:
                    file_key = obj['Key']
                    try:
                        response = s3_client.get_object(Bucket='pokemon-lake', Key=file_key)
                        parquet_data = response['Body'].read()
                        df = pd.read_parquet(io.BytesIO(parquet_data))

                        for _, row in df.iterrows():
                            record = (
                                row.get('ability_id'),
                                row.get('name'),
                                row.get('effect_name'),
                                row.get('short_effect_name'),
                                row.get('generation'),
                                row.get('is_main_series'),
                                row.get('batch_id'),
                                row.get('ingested_at'),
                                row.get('source_url'),
                                row.get('ingestion_date'),
                                row.get('bronze_processed_at'),
                                row.get('source_file'),
                                batch_id,
                                datetime.now().isoformat()
                            )
                            all_ability_records.append(record)

                    except Exception as e:
                        logging.error(f"Error processing abilities file {file_key}: {e}")

                if all_ability_records:
                    postgres_hook.run(
                        "DELETE FROM staging.stg_abilities WHERE DATE(loaded_at) = CURRENT_DATE"
                    )

                    ability_fields = [
                        'ability_id', 'name', 'effect_name', 'short_effect_name',
                        'generation', 'is_main_series', 'bronze_batch_id',
                        'ingested_at', 'source_url', 'ingestion_date',
                        'bronze_processed_at', 'source_file', 'staging_batch_id', 'loaded_at'
                    ]

                    postgres_hook.insert_rows(
                        table='staging.stg_abilities',
                        rows=all_ability_records,
                        target_fields=ability_fields,
                        commit_every=1000,
                        fast_executemany=True
                    )

                    results['abilities'] = {
                        'staging_batch_id': batch_id,
                        'records_loaded': len(all_ability_records)
                    }

            pokemon_abilities_prefix = f"bronze/pokemon_abilities/date={today}/"
            logging.info(f"Loading pokemon abilities bronze data from prefix: {pokemon_abilities_prefix}")

            pokemon_abilities_objects = []
            continuation_token = None

            while True:
                list_params = {
                    'Bucket': 'pokemon-lake',
                    'Prefix': pokemon_abilities_prefix
                }
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                response = s3_client.list_objects_v2(**list_params)
                page_objects = response.get('Contents', [])
                parquet_objects = [obj for obj in page_objects if obj['Key'].endswith('.parquet')]
                pokemon_abilities_objects.extend(parquet_objects)

                if not response.get('IsTruncated', False):
                    break
                continuation_token = response.get('NextContinuationToken')

            if pokemon_abilities_objects:
                batch_id = str(uuid.uuid4())[:8]
                all_pokemon_ability_records = []

                for obj in pokemon_abilities_objects:
                    file_key = obj['Key']
                    try:
                        response = s3_client.get_object(Bucket='pokemon-lake', Key=file_key)
                        parquet_data = response['Body'].read()
                        df = pd.read_parquet(io.BytesIO(parquet_data))

                        for _, row in df.iterrows():
                            record = (
                                row.get('pokemon_name'),
                                row.get('ability_id'),
                                row.get('ability_name'),
                                row.get('is_hidden'),
                                row.get('slot'),
                                row.get('batch_id'),
                                row.get('ingested_at'),
                                row.get('source_url'),
                                row.get('ingestion_date'),
                                row.get('bronze_processed_at'),
                                row.get('source_file'),
                                batch_id,
                                datetime.now().isoformat()
                            )
                            all_pokemon_ability_records.append(record)

                    except Exception as e:
                        logging.error(f"Error processing pokemon abilities file {file_key}: {e}")

                if all_pokemon_ability_records:
                    postgres_hook.run(
                        "DELETE FROM staging.stg_pokemon_abilities WHERE DATE(loaded_at) = CURRENT_DATE"
                    )

                    pokemon_ability_fields = [
                        'pokemon_name', 'ability_id', 'ability_name', 'is_hidden', 'slot',
                        'bronze_batch_id', 'ingested_at', 'source_url', 'ingestion_date',
                        'bronze_processed_at', 'source_file', 'staging_batch_id', 'loaded_at'
                    ]

                    postgres_hook.insert_rows(
                        table='staging.stg_pokemon_abilities',
                        rows=all_pokemon_ability_records,
                        target_fields=pokemon_ability_fields,
                        commit_every=1000,
                        fast_executemany=True
                    )

                    results['pokemon_abilities'] = {
                        'staging_batch_id': batch_id,
                        'records_loaded': len(all_pokemon_ability_records)
                    }

            logging.info(f"=== ABILITIES BRONZE TO STAGING COMPLETE ===")
            logging.info(f"Results: {results}")

            return results

        pokemon_staging = load_pokemon_bronze_to_staging()
        abilities_staging = load_abilities_bronze_to_staging()

    start = DummyOperator(task_id="start_dag")
    stage_tables = setup_elt_tables()
    extract_bronze = bronze_to_staging()
    end = DummyOperator(task_id="end_dag")

    start >> stage_tables >> extract_bronze >> end


dag = bronze_to_silver_dag()