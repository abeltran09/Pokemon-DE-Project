CREATE TABLE IF NOT EXISTS staging.stg_abilities (
    ability_id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    effect_name TEXT,
    short_effect_name TEXT,
    generation VARCHAR(50),
    is_main_series BOOLEAN DEFAULT TRUE,
    bronze_batch_id VARCHAR(50),
    ingested_at TIMESTAMP,
    source_url TEXT,
    ingestion_date DATE,
    bronze_processed_at TIMESTAMP,
    source_file TEXT,
    staging_batch_id VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);