from datetime import datetime, date
import logging
import requests
import time
from minio_client import store_to_minio, get_s3_client
import uuid
import json


class PokemonDataIngester:

    def __init__(self,
                 base_api_url="https://pokeapi.co/api/v2",
                 default_delay=0.5,
                 default_batch_size=50,
                 bucket_name="pokemon-lake"):
        self.base_api_url = base_api_url
        self.default_delay=default_delay
        self.default_batch_size = default_batch_size
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3_client = get_s3_client()

    def ingest_to_raw(self,
                      ingest_type,
                      delay=None,
                      batch_size=None,
                      target_date=None):
        delay = delay if delay is not None else self.default_delay
        batch_size = batch_size if batch_size is not None else self.default_batch_size
        target_date = target_date or date.today()


        batch_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()

        self.logger.info(f"Starting {ingest_type} ingestion with batch_id: {batch_id}")

        all_items, initial_request_count = self.get_all_item(ingest_type, delay)

        stats = self.process_items(
            all_items,
            ingest_type,
            target_date,
            batch_id,
            delay,
            batch_size,
            initial_request_count,
            start_time
            )

        self.log_completion_stats(stats, start_time)

        return stats

    def get_all_item(self, ingest_type, delay):
        all_items = []
        request_count = 0
        api_url = f"{self.base_api_url}/{ingest_type}/?limit=100&offset=0"

        self.logger.info("Starting item list collection...")

        while api_url:

            try:
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                request_count += 1

                data = response.json()
                all_items.extend(data["results"])
                api_url = data.get("next")

                if delay and api_url:
                    time.sleep(delay)

            except requests.RequestException as e:
                self.logger.error(f"Failed to fetch item list: {e}")
                raise

        self.logger.info(f"Found {len(all_items)} {ingest_type}s. Made {request_count} list requests.")
        return all_items, request_count

    def process_items(self,
                      items,
                      ingest_type,
                      target_date,
                      batch_id,
                      delay,
                      batch_size,
                      initial_request_count,
                      start_time):

        total_items = len(items)
        request_count = initial_request_count
        successful_stores = 0
        failed_stores = 0

        self.logger.info(f"Processing {total_items} {ingest_type}s to raw layer")

        for i, item in enumerate(items):
            try:
                item_url = item["url"]
                item_name = item["name"]
                item_id = item_url.split('/')[-2]


                if (i + 1) % batch_size == 0:
                    elapse = datetime.now() - start_time
                    self.logger.info(f"Progress: {i + 1}/{total_items} ({(i + 1) / total_items * 100:.1f}%) - "
                                     f"Elapse: {elapse.total_seconds():1f}s")

                response = requests.get(item_url, timeout=10)
                response.raise_for_status()
                request_count += 1

                data = response.json()


                data['_ingestion_metadata'] = {
                    'batch_id': batch_id,
                    'ingested_at': datetime.now().isoformat(),
                    'source_url': item_url,
                    'ingestion_date': target_date.isoformat()
                }

                self.store_to_raw_layer(
                    data,
                    ingest_type,
                    item_id,
                    item_name,
                    target_date,
                    batch_id
                )

                successful_stores += 1

                if delay:
                    time.sleep(delay)

            except requests.RequestException as e:
                failed_stores += 1
                self.logger.warning(f"Failed to fetch {item_name}: {e}")
                continue

            except Exception as e:
                failed_stores += 1
                self.logger.warning(f"Failed to store {item_name}: {e}")
                continue

        return {
            'total_requests': request_count,
            'successful_stores': successful_stores,
            'failed_stores': failed_stores,
            'total_items': total_items
        }

    def store_to_raw_layer(self,
                           data,
                           ingest_type,
                           item_id,
                           item_name,
                           target_date,
                           batch_id):

        raw_key = (
            f"raw/{ingest_type}/"
            f"date={target_date.strftime('%Y-%m-%d')}/"
            f"batch_id={batch_id}/"
            f"{item_id}_{item_name}.json"
        )

        try:
            json_data = json.dumps(data, indent=2).encode('utf-8')

            store_to_minio(self.s3_client, json_data, raw_key, self.bucket_name)

            self.logger.debug(f"Stored {raw_key}")

        except Exception as e:
            self.logger.error(f"Failed to store {raw_key}: {e}")
            raise

    def log_completion_stats(self, stats, start_time):
        """Log completion statistics"""
        total_time = datetime.now() - start_time

        self.logger.info(f"Ingestion completed!")
        self.logger.info(f"Total requests made: {stats['total_requests']}")
        self.logger.info(f"Successful stores: {stats['successful_stores']}")
        self.logger.info(f"Failed stores: {stats['failed_stores']}")
        self.logger.info(f"Total runtime: {total_time.total_seconds():.1f} seconds")
        self.logger.info(f"Average requests/second: {stats['total_requests'] / total_time.total_seconds():.2f}")

    def ingest_pokemon_base(self, delay=None, batch_size=None, target_date=None):
        return self.ingest_to_raw(
            ingest_type="pokemon",
            delay=delay,
            batch_size=batch_size,
            target_date=target_date
        )

    def ingest_pokemon_abilities(self, delay=None, batch_size=None, target_date=None):
        return self.ingest_to_raw(
            ingest_type="ability",
            delay=delay,
            batch_size=batch_size,
            target_date=target_date
        )
