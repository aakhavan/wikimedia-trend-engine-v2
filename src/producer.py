import json
import boto3
import requests
import logging
from botocore.exceptions import ClientError
from src.config_loader import config

# Configure logging for better visibility
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
        self.kinesis_stream_name = self.config['aws']['kinesis_stream_name']
        self.aws_region = self.config['aws']['region']
        self.kinesis_client = self._get_kinesis_client()
        # Using a requests.Session is a best practice for performance
        self.session = requests.Session()

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
        """
        Sends a single JSON record to the Kinesis stream.
        Uses the event's domain as the partition key for good data distribution.
        """
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

    def run(self):
        """
        Main run loop. Connects to the stream and processes events indefinitely.
        """
        if not self.kinesis_client:
            logging.error("Kinesis client not available. Aborting run.")
            return

        logging.info(f"Connecting to Wikimedia SSE stream: {self.stream_url}")
        try:
            with self.session.get(self.stream_url, stream=True, timeout=30) as response:
                response.raise_for_status()
                logging.info("Connection successful. Listening for events...")
                for line in response.iter_lines():
                    if line and line.startswith(b'data: '):
                        try:
                            json_str = line.decode('utf-8')[6:]
                            record = json.loads(json_str)
                            logging.info(f"Received event for domain: {record.get('meta', {}).get('domain')}")
                            self._send_to_kinesis(record)
                        except json.JSONDecodeError:
                            logging.debug(f"Skipping non-JSON data line: {line}")
                        except Exception as e:
                            logging.error(f"An unexpected error occurred while processing a line: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Fatal error connecting to the Wikimedia stream: {e}")


if __name__ == "__main__":
    producer = WikimediaProducer()
    producer.run()