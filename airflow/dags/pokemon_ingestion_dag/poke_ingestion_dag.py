import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task, task_group
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import requests
from airflow.models import Variable
from airflow.api.client.local_client import Client
from minio_client import store_to_minio, get_s3_client
from dotenv import load_dotenv
import time
import logging
from pokemon_data_ingester import PokemonDataIngester
from pokemon_ingestion_dag.transformations import *
import json
from airflow.exceptions import AirflowException
import uuid
import pandas as pd
import json
from datetime import datetime
import io

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
POKEMON_BUCKET = "pokemons"
ABILITY_BUCKET = "abilities"


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


    @task_group(group_id="bronze_processing")
    def bronze_layer_processing():
        @task
        def process_pokemon_to_bronze():
            s3_client = get_s3_client()

            today = datetime.now().strftime('%Y-%m-%d')
            raw_prefix = f"raw/pokemon/date={today}/"

            logging.info("Starting MinIO object listing with pagination for bronze processing...")

            all_objects = []
            continuation_token = None
            page_count = 0

            while True:
                page_count += 1
                logging.info(f"Fetching page {page_count} from MinIO...")

                # Build request parameters
                list_params = {
                    'Bucket': 'pokemon-lake',
                    'Prefix': raw_prefix
                }
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                # Get objects page
                response = s3_client.list_objects_v2(**list_params)

                # Add objects from this page
                page_objects = response.get('Contents', [])
                all_objects.extend(page_objects)
                logging.info(f"Page {page_count}: Found {len(page_objects)} objects (Total so far: {len(all_objects)})")

                # Check if there are more pages
                if not response.get('IsTruncated', False):
                    logging.info("No more pages to fetch")
                    break

                # Get token for next page
                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    logging.warning("IsTruncated=True but no NextContinuationToken found")
                    break

            logging.info(f"=== PAGINATION COMPLETE ===")
            logging.info(f"Total pages fetched: {page_count}")
            logging.info(f"Total raw files found: {len(all_objects)}")

            if not all_objects:
                logging.warning("No raw files found for bronze processing")
                return None

            # Process all raw files to bronze records
            all_records = []
            processed_count = 0
            error_count = 0

            logging.info(f"Processing {len(all_objects)} raw files to bronze format...")

            for i, obj in enumerate(all_objects, 1):
                file_key = obj['Key']

                # Log progress every 100 files
                if i % 100 == 0 or i == len(all_objects):
                    logging.info(f"Processing file {i}/{len(all_objects)}: {file_key}")

                try:
                    # Get raw file from MinIO
                    response = s3_client.get_object(Bucket='pokemon-lake', Key=file_key)
                    raw_data = json.load(response['Body'])

                    # Extract ingestion metadata
                    metadata = raw_data.get('_ingestion_metadata', {})
                    types = [t["type"]["name"] for t in raw_data.get("types", [])]
                    type_1 = types[0] if len(types) > 0 else None
                    type_2 = types[1] if len(types) > 1 else None

                    # Extract stats
                    stats_dict = {s["stat"]["name"]: s["base_stat"] for s in raw_data.get("stats", [])}

                    # Pick a main sprite
                    sprite_url = raw_data.get("sprites", {}).get("other", {}).get("official-artwork", {}).get(
                        "front_default", "None")

                    # Basic schema enforcement and flattening
                    record = {
                        'pokemon_id': raw_data.get('id'),
                        'name': raw_data.get('name'),
                        'height': raw_data.get('height'),
                        'weight': raw_data.get('weight'),
                        'base_experience': raw_data.get('base_experience'),
                        "type_1": type_1,
                        "type_2": type_2,
                        "hp": stats_dict.get("hp"),
                        "attack": stats_dict.get("attack"),
                        "defense": stats_dict.get("defense"),
                        "special_attack": stats_dict.get("special-attack"),
                        "special_defense": stats_dict.get("special-defense"),
                        "speed": stats_dict.get("speed"),
                        "sprite_url": sprite_url,
                        'batch_id': metadata.get('batch_id'),
                        'ingested_at': metadata.get('ingested_at'),
                        'source_url': metadata.get('source_url'),
                        'ingestion_date': metadata.get('ingestion_date'),
                        'bronze_processed_at': datetime.now().isoformat(),
                        'source_file': file_key
                    }
                    all_records.append(record)
                    processed_count += 1

                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON in raw file {file_key}: {e}")
                    error_count += 1
                    continue
                except Exception as e:
                    logging.error(f"Error processing raw file {file_key}: {e}")
                    error_count += 1
                    continue

            # Log processing summary
            logging.info("=== RAW TO BRONZE PROCESSING SUMMARY ===")
            logging.info(f"Total raw files found: {len(all_objects)}")
            logging.info(f"Successfully processed: {processed_count}")
            logging.info(f"Processing errors: {error_count}")
            logging.info(f"Bronze records created: {len(all_records)}")

            if not all_records:
                raise Exception("No valid records to save to bronze layer")

            # Convert to DataFrame and save as Parquet
            df = pd.DataFrame(all_records)

            # Create bronze layer path with same date partitioning
            parquet_key = f"bronze/pokemon/date={today}/pokemon_bronze_{today}.parquet"

            # Convert DataFrame to Parquet bytes and upload
            parquet_buffer = df.to_parquet(index=False)
            s3_client.put_object(
                Bucket='pokemon-lake',
                Key=parquet_key,
                Body=parquet_buffer,
                ContentType='application/octet-stream'
            )

            logging.info(f"=== BRONZE LAYER CREATED ===")
            logging.info(f"Bronze file: {parquet_key}")
            logging.info(f"Records saved: {len(all_records)}")
            logging.info(f"File size: {len(parquet_buffer)} bytes")

            # Return bronze processing stats
            return {
                'bronze_file': parquet_key,
                'records_processed': len(all_records),
                'raw_files_processed': processed_count,
                'processing_errors': error_count,
                'processing_date': today
            }


        @task
        def process_abilities_to_bronze():
            s3_client = get_s3_client()

            today = datetime.now().strftime('%Y-%m-%d')
            raw_prefix = f"raw/ability/date={today}/"

            logging.info("Starting MinIO object listing with pagination for bronze processing...")

            all_objects = []
            continuation_token = None
            page_count = 0

            while True:
                page_count += 1
                logging.info(f"Fetching page {page_count} from MinIO...")

                # Build request parameters
                list_params = {
                    'Bucket': 'pokemon-lake',
                    'Prefix': raw_prefix
                }
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                # Get objects page
                response = s3_client.list_objects_v2(**list_params)

                # Add objects from this page
                page_objects = response.get('Contents', [])
                all_objects.extend(page_objects)
                logging.info(
                    f"Page {page_count}: Found {len(page_objects)} objects (Total so far: {len(all_objects)})")

                # Check if there are more pages
                if not response.get('IsTruncated', False):
                    logging.info("No more pages to fetch")
                    break

                # Get token for next page
                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    logging.warning("IsTruncated=True but no NextContinuationToken found")
                    break

            logging.info(f"=== PAGINATION COMPLETE ===")
            logging.info(f"Total pages fetched: {page_count}")
            logging.info(f"Total raw files found: {len(all_objects)}")

            if not all_objects:
                logging.warning("No raw files found for bronze processing")
                return None

            # Process all raw files to bronze records
            ability_records = []
            pokemon_ability_records = []
            processed_count = 0
            error_count = 0

            logging.info(f"Processing {len(all_objects)} raw files to bronze format...")

            for i, obj in enumerate(all_objects, 1):
                file_key = obj['Key']

                # Log progress every 100 files
                if i % 100 == 0 or i == len(all_objects):
                    logging.info(f"Processing file {i}/{len(all_objects)}: {file_key}")

                try:
                    # Get raw file from MinIO
                    response = s3_client.get_object(Bucket='pokemon-lake', Key=file_key)
                    raw_data = json.load(response['Body'])

                    # Extract ingestion metadata
                    metadata = raw_data.get('_ingestion_metadata', {})
                    ability_id = raw_data.get("id")
                    ability_name = raw_data.get("name")

                    effect_name = None
                    effect_short_name = None

                    for entry in raw_data.get("effect_entries", []):
                        if entry.get("language", {}).get("name") == 'en':
                            effect_name = entry.get("effect")
                            effect_short_name = entry.get("short_effect")
                            break


                    # Basic schema enforcement and flattening
                    record = {
                        'ability_id': ability_id,
                        'name': ability_name,
                        'effect_name': effect_name,
                        'short_effect_name': effect_short_name,
                        'generation': raw_data.get('generation', {}).get('name'),
                        'is_main_series': raw_data.get('is_main_series', False),
                        'batch_id': metadata.get('batch_id'),
                        'ingested_at': metadata.get('ingested_at'),
                        'source_url': metadata.get('source_url'),
                        'ingestion_date': metadata.get('ingestion_date'),
                        'bronze_processed_at': datetime.now().isoformat(),
                        'source_file': file_key
                    }
                    ability_records.append(record)

                    pokemons_with_ability = raw_data.get('pokemon', [])
                    for pokemon in pokemons_with_ability:
                        pokemon_name = pokemon.get('pokemon', {}).get('name')
                        is_hidden = pokemon.get('is_hidden')
                        slot = pokemon.get('slot', 1)

                        pokemon_record = {
                            'pokemon_name': pokemon_name,
                            'ability_id': ability_id,
                            'ability_name': ability_name,
                            'is_hidden': is_hidden,
                            'slot': slot,
                            'batch_id': metadata.get('batch_id'),
                            'ingested_at': metadata.get('ingested_at'),
                            'source_url': metadata.get('source_url'),
                            'ingestion_date': metadata.get('ingestion_date'),
                            'bronze_processed_at': datetime.now().isoformat(),
                            'source_file': file_key
                        }
                        pokemon_ability_records.append(pokemon_record)

                    processed_count += 1

                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON in raw file {file_key}: {e}")
                    error_count += 1
                    continue
                except Exception as e:
                    logging.error(f"Error processing raw file {file_key}: {e}")
                    error_count += 1
                    continue

            results = {}

            if ability_records:
                ability_df = pd.DataFrame(ability_records)

                ability_key = f"bronze/abilities/date={today}/abilities_bronze_{today}.parquet"

                # Convert DataFrame to Parquet bytes and upload
                parquet_buffer = ability_df.to_parquet(index=False)
                s3_client.put_object(
                    Bucket='pokemon-lake',
                    Key=ability_key,
                    Body=parquet_buffer,
                    ContentType='application/octet-stream'
                )
                results['abilities_file'] = ability_key
                results['abilities_count'] = len(ability_records)

            if pokemon_ability_records:
                pokemon_abilities_df = pd.DataFrame(pokemon_ability_records)

                pokemon_abilities_key = f"bronze/pokemon_abilities/date={today}/pokemon_abilities_bronze_{today}.parquet"

                # Convert DataFrame to Parquet bytes and upload
                parquet_buffer = pokemon_abilities_df.to_parquet(index=False)
                s3_client.put_object(
                    Bucket='pokemon-lake',
                    Key=pokemon_abilities_key,
                    Body=parquet_buffer,
                    ContentType='application/octet-stream'
                )
                results['pokemon_abilities_file'] = pokemon_abilities_df
                results['pokemon_abilities_count'] = len(pokemon_ability_records)

            logging.info(f"=== ABILITIES BRONZE PROCESSING COMPLETE ===")
            logging.info(f"Abilities processed: {len(ability_records)}")
            logging.info(f"Pokemon-Ability relationships: {len(pokemon_ability_records)}")
            logging.info(f"Processing errors: {error_count}")

            return results

        pokemon_bronze = process_pokemon_to_bronze()
        abilities_bronze = process_abilities_to_bronze()




    start = DummyOperator(task_id="start_dag")

    pokemon_ingestion_group = ingest_pokemon_data()
    pokemon_transform_to_bronze = bronze_layer_processing()



    end = DummyOperator(
        task_id="end_dag"
    )

    start >> pokemon_ingestion_group >> pokemon_transform_to_bronze >> end

dag = pokemon_ingest_dag()