CREATE TABLE IF NOT EXISTS staging.stg_pokemon_abilities (
    pokemon_name VARCHAR(100) NOT NULL,
    ability_id INTEGER NOT NULL,
    ability_name VARCHAR(100) NOT NULL,
    is_hidden BOOLEAN DEFAULT FALSE,
    slot INTEGER,
    bronze_batch_id VARCHAR(50),
    ingested_at TIMESTAMP,
    source_url TEXT,
    ingestion_date DATE,
    bronze_processed_at TIMESTAMP,
    source_file TEXT,
    staging_batch_id VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);