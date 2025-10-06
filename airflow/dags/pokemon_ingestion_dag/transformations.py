from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json
import pandas as pd
from io import BytesIO
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from minio_client import get_s3_client
from datetime import timedelta



POKEMON_BUCKET = "pokemons"
ABILITY_BUCKET = "abilities"

# SQL DDL statements as constants
DDL_POKEMON_BASE = """
DROP TABLE IF EXISTS pokemon_base; 
CREATE TABLE IF NOT EXISTS pokemon_base ( 
    id INT PRIMARY KEY, 
    name TEXT, 
    height INT, 
    weight INT, 
    base_experience INT, 
    type_1 TEXT, 
    type_2 TEXT, 
    hp INT, 
    attack INT, 
    defense INT, 
    special_attack INT, 
    special_defense INT, 
    speed INT, 
    sprite_url TEXT 
);
"""

DDL_POKEMON_MOVES = """
DROP TABLE IF EXISTS pokemon_moves; 
CREATE TABLE IF NOT EXISTS pokemon_moves ( 
    id SERIAL PRIMARY KEY, 
    pokemon_id INT, 
    pokemon_name TEXT,
    move_name TEXT, 
    level_learned_at INT, 
    learn_method TEXT, 
    version_group TEXT 
);
"""

DDL_POKEMON_ABILITY = """
CREATE TABLE pokemon_abilities (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    effect_name TEXT,
    short_effect_name TEXT,
    pokemon_with_ability TEXT[]
);
"""

def extract_all_and_stage_pokemon():
    """Fetch all raw Pokémon JSONs from MinIO and return as a list of dicts."""

    s3 = get_s3_client()
    objects = s3.list_objects_v2(Bucket=POKEMON_BUCKET)

    all_data = []
    for obj in objects.get("Contents", []):
        file_key = obj["Key"]
        response = s3.get_object(Bucket=POKEMON_BUCKET, Key=file_key)
        raw_json = json.load(response["Body"])
        all_data.append(raw_json)

    trandformed_df = transform_pokemon_base(all_data)

    return trandformed_df



def transform_pokemon_base(data):
    transformed = [transform_pokemon_base_helper(p) for p in data]
    return pd.DataFrame(transformed)


def transform_pokemon_moves(**kwargs):
    """Turn raw JSONs into a dataframe for pokemon_moves table."""
    all_data = kwargs['ti'].xcom_pull(task_ids='extract_pokemon_from_minio')
    transformed = []
    for pokemon_json in all_data:
        # Get the DataFrame from helper and append rows
        move_df = transform_pokemon_moves_helper(pokemon_json)
        transformed.extend(move_df.to_dict('records'))
    return pd.DataFrame(transformed)


def transform_pokemon_moves_helper(pokemon_json):
    """Turn raw JSON into a DataFrame of pokemon moves"""
    moves_data = []

    pokemon_id = pokemon_json.get("id")
    pokemon_name = pokemon_json.get("name")

    for move in pokemon_json.get("moves", []):
        move_name = move["move"]["name"]

        for detail in move.get("version_group_details", []):
            moves_data.append({
                "pokemon_id": pokemon_id,
                "pokemon_name": pokemon_name,
                "move_name": move_name,
                "level_learned_at": detail["level_learned_at"],
                "learn_method": detail["move_learn_method"]["name"],
                "version_group": detail["version_group"]["name"]
            })

    return pd.DataFrame(moves_data)


def transform_pokemon_base_helper(pokemon_json):
    # Extract types
    types = [t["type"]["name"] for t in pokemon_json.get("types", [])]
    type_1 = types[0] if len(types) > 0 else None
    type_2 = types[1] if len(types) > 1 else None

    # Extract stats
    stats_dict = {s["stat"]["name"]: s["base_stat"] for s in pokemon_json.get("stats", [])}

    # Pick a main sprite
    sprite_url = pokemon_json.get("sprites", {}).get("other", {}).get("official-artwork", {}).get("front_default", "None")

    return {
        "id": pokemon_json.get("id"),
        "name": pokemon_json.get("name"),
        "height": pokemon_json.get("height"),
        "weight": pokemon_json.get("weight"),
        "base_experience": pokemon_json.get("base_experience"),
        "type_1": type_1,
        "type_2": type_2,
        "hp": stats_dict.get("hp"),
        "attack": stats_dict.get("attack"),
        "defense": stats_dict.get("defense"),
        "special_attack": stats_dict.get("special-attack"),
        "special_defense": stats_dict.get("special-defense"),
        "speed": stats_dict.get("speed"),
        "sprite_url": sprite_url
    }


def extract_all_pokemon_abilities():
    minio = get_s3_client()
    objects = minio.list_objects_v2(Bucket=ABILITY_BUCKET)

    all_data = []
    for obj in objects.get("Contents", []):
        file_key = obj["Key"]
        response = minio.get_object(Bucket=ABILITY_BUCKET, Key=file_key)
        raw_json = json.load(response["Body"])
        all_data.append(raw_json)
    return all_data


def transform_all_abilities(**kwargs):
    all_data = kwargs["ti"].xcom_pull("extract_pokemon_ability_from_minio")
    transformed = [transform_all_abilities_helper(ability) for ability in all_data]
    return pd.DataFrame(transformed)

def transform_all_abilities_helper(ability_json):
    id = ability_json.get("id")
    ability_name = ability_json.get("name")

    effect_name = None
    effect_short_name = None

    for entry in ability_json.get("effect_entries", []):
        if entry.get("language", {}).get("name") == 'en':
            effect_name = entry.get("effect")
            effect_short_name = entry.get("short_effect")
            break

    pokemon_with_ability = [
        pokemon_entry["pokemon"]["name"]
        for pokemon_entry in ability_json.get("pokemon", [])
    ]

    return {
        "id": id,
        "name": ability_name,
        "effect_name": effect_name,
        "short_effect_name": effect_short_name,
        "pokemon_with_ability": pokemon_with_ability
    }


def load_dataframe_to_postgres(df, table_name, create_table_sql):
    """Generic loader to Postgres with upsert logic."""
    if df is None or df.empty:
        print(f"No data for {table_name}")
        return

    hook = PostgresHook(postgres_conn_id="poke_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(create_table_sql)
    conn.commit()

    # Insert dynamically
    for _, row in df.iterrows():
        placeholders = ",".join(["%s"] * len(row))
        columns = ",".join(df.columns)

        # For pokemon_moves table, we don't want to update on conflict since it has SERIAL id
        if table_name == "pokemon_moves":
            sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders});
            """
        else:
            # For pokemon_base, we can do upserts
            updates = ",".join([f"{col}=EXCLUDED.{col}" for col in df.columns if col != "id"])
            sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET {updates};
            """
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

def load_pokemon_base_data(**kwargs):
    """Load pokemon base data to postgres."""
    df = kwargs['ti'].xcom_pull(task_ids='transform_base_pokemon_task')
    load_dataframe_to_postgres(df, "pokemon_base", DDL_POKEMON_BASE)


def load_pokemon_moves_data(**kwargs):
    """Load pokemon moves data to postgres."""
    df = kwargs['ti'].xcom_pull(task_ids='transform_pokemon_moves_task')
    load_dataframe_to_postgres(df, "pokemon_moves", DDL_POKEMON_MOVES)


def load_pokemon_ability_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="transform_pokemon_abilities_task")
    load_dataframe_to_postgres((df, "pokemon_abilities"), DDL_POKEMON_ABILITY)