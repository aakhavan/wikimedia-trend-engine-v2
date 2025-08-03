import json
import boto3
import requests
import logging
import time
from botocore.exceptions import ClientError
from src.config_loader import config


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class WikimediaProducer:
    """
    Manages the connection to the Wikimedia SSE stream and sends data to Kinesis.
    This class encapsulates the Kinesis client and the request session state.
    """

    def __init__(self):
        """Initializes the producer, setting up configuration and clients."""
        self.config = config
        self.stream_url = self.config['wikimedia']['stream_url']
        # --- THIS IS THE FIX ---
        # Read the target domains from config. It's now a list.
        self.target_domains = self.config['wikimedia'].get('target_domains')
        self.kinesis_stream_name = self.config['aws']['kinesis_stream_name']
        self.aws_region = self.config['aws']['region']
        self.kinesis_client = self._get_kinesis_client()
        self.session = requests.Session()
        self.retry_delay = 5

    def _get_kinesis_client(self):
        """Initializes and returns a Boto3 Kinesis client."""
        try:
            client = boto3.client('kinesis', region_name=self.aws_region)
            client.describe_stream(StreamName=self.kinesis_stream_name)
            logging.info(f"Successfully connected to Kinesis stream: {self.kinesis_stream_name}")
            return client
        except ClientError as e:
            logging.error(f"Error connecting to Kinesis stream '{self.kinesis_stream_name}': {e}")
            logging.error("Please ensure the Kinesis stream exists and your credentials are correct.")
            raise

    def _send_to_kinesis(self, record: dict):
        """Sends a single JSON record to the Kinesis stream."""
        try:
            partition_key = record.get('meta', {}).get('domain', 'default_domain')
            data = json.dumps(record).encode('utf-8')
            self.kinesis_client.put_record(
                StreamName=self.kinesis_stream_name,
                Data=data,
                PartitionKey=partition_key
            )
        except ClientError as e:
            logging.warning(f"Could not put record to Kinesis. SKIPPING. Error: {e}")

    def run(self, run_duration_seconds: int = None):
        """
        Main run loop. Connects to the stream and processes events.
        Can run indefinitely or for a fixed duration, which is ideal for development.
        :param run_duration_seconds: If set, the producer will stop after this many seconds.
        """
        if not self.kinesis_client:
            logging.error("Kinesis client not available. Aborting run.")
            return


        if self.target_domains:
            logging.info(f"Filtering is ACTIVE. Only ingesting data for domains: {self.target_domains}")
        else:
            logging.info("No target domains specified. Ingesting data for ALL domains.")

        start_time = time.time()

        while True:
            if run_duration_seconds and (time.time() - start_time) > run_duration_seconds:
                logging.info(f"Run duration of {run_duration_seconds} seconds reached. Shutting down producer.")
                break
            try:
                logging.info(f"Connecting to Wikimedia SSE stream: {self.stream_url}")
                with self.session.get(self.stream_url, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    logging.info("Connection successful. Listening for events...")
                    for line in response.iter_lines():
                        if line and line.startswith(b'data: '):
                            try:
                                json_str = line.decode('utf-8')[6:]
                                record = json.loads(json_str)
                                current_domain = record.get('meta', {}).get('domain')

                                if not self.target_domains or current_domain in self.target_domains:
                                    logging.info(f"Processing event for domain: {current_domain}")
                                    self._send_to_kinesis(record)

                            except json.JSONDecodeError:
                                logging.debug(f"Skipping non-JSON data line: {line}")
                            except Exception as e:
                                logging.error(f"An unexpected error occurred while processing a line: {e}")

            # Catch any network-related exceptions from the requests library
            except requests.exceptions.RequestException as e:
                logging.warning(f"Connection dropped: {e}. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
            # Catch any other unexpected errors to prevent the script from crashing
            except Exception as e:
                logging.error(f"An unexpected fatal error occurred: {e}. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)


if __name__ == "__main__":
    # producer = WikimediaProducer()
    # producer.run()
    logging.info("Starting producer in development mode for 10 minutes")
    producer = WikimediaProducer()
    producer.run(run_duration_seconds=600)  # Run for 10 minutes