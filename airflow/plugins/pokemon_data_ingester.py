from datetime import datetime
import logging
import requests
import time
from minio_client import store_to_minio


class PokemonDataIngester:

    def __init__(self, base_api_url="https://pokeapi.co/api/v2", default_delay=0.5, default_batch_size=50):
        self.base_api_url = base_api_url
        self.default_delay=default_delay
        self.default_batch_size = default_batch_size
        self.logger = logging.getLogger(self.__class__.__name__)

    def ingest(self, ingest_type, bucket=None, delay=None, batch_size=None):
        delay = delay if delay is not None else self.default_delay
        batch_size = batch_size if batch_size is not None else self.default_batch_size

        if bucket is None:
            bucket = f"{ingest_type}"

        start_time = datetime.now()
        api_url = f"{self.base_api_url}/{ingest_type}/?limit=100&offset=0"

        all_items = self.get_all_item(api_url, ingest_type, delay)

        stats = self.process_items(all_items, ingest_type, bucket, delay, batch_size, start_time)

        self.log_completion_stats(stats, start_time)

        return stats

    def get_all_item(self, api_url, ingest_type, delay):
        all_items = []
        request_count = 0

        self.logger.info("Starting ability list collection...")

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
                self.logger.error(f"Failed to fetch ability list: {e}")
                raise

        self.logger.info(f"Found {len(all_items)} {ingest_type}s. Made {request_count} list requests.")
        return all_items, request_count

    def process_items(self, all_items, ingest_type, bucket, delay, batch_size, start_time):
        items, initial_request_count = all_items
        total_items = len(items)
        request_count = initial_request_count
        successful_stores = 0
        failed_stores = 0

        for i, item in enumerate(items):
            try:
                item_url = item["url"]
                item_name = item["name"]

                if (i + 1) % batch_size == 0:
                    elapse = datetime.now() - start_time
                    self.logger.info(f"Progress: {i + 1}/{total_items} ({(i + 1) / total_items * 100:.1f}%) - "
                                     f"Elapse: {elapse.total_seconds():1f}s")

                response = requests.get(item_url, timeout=10)
                response.raise_for_status()
                request_count += 1

                data = response.json()
                file_name = f"{ingest_type}_{data['id']}_{item_name}"
                store_to_minio(data, file_name, bucket)
                successful_stores += 1

                if delay:
                    time.sleep(delay)

            except requests.RequestException as e:
                failed_stores += 1
                self.logger.warning(f"Failed to fetch {item_name}: {e}")
                continue

            except Exception as e:
                failed_stores += 1
                self.logger.warning(f"Failed to store {file_name}: {e}")
                continue

        return {
            'total_requests': request_count,
            'successful_stores': successful_stores,
            'failed_stores': failed_stores,
            'total_items': total_items
        }

    def log_completion_stats(self, stats, start_time):
        """Log completion statistics"""
        total_time = datetime.now() - start_time

        self.logger.info(f"Ingestion completed!")
        self.logger.info(f"Total requests made: {stats['total_requests']}")
        self.logger.info(f"Successful stores: {stats['successful_stores']}")
        self.logger.info(f"Failed stores: {stats['failed_stores']}")
        self.logger.info(f"Total runtime: {total_time.total_seconds():.1f} seconds")
        self.logger.info(f"Average requests/second: {stats['total_requests'] / total_time.total_seconds():.2f}")


    def ingest_pokemon_base(self, bucket="pokemons", **kwargs):
        return self.ingest(ingest_type="pokemon", bucket=bucket, **kwargs)


    def ingest_pokemon_abilities(self, bucket="abilities", **kwargs):
        return self.ingest(ingest_type="ability", bucket=bucket, **kwargs)
