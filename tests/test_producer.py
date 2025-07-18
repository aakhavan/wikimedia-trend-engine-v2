import json
import pytest
from src.producer import WikimediaProducer

# A sample valid data line from the Wikimedia stream (as bytes)
SAMPLE_SSE_EVENT = b'data: {"meta":{"domain":"en.wikipedia.org"},"id":123,"type":"edit"}'
# A keep-alive line that should be ignored
SAMPLE_KEEP_ALIVE = b':ok'


@pytest.fixture
def mock_producer(mocker):
    """
    A pytest fixture that creates an instance of WikimediaProducer
    with its external dependencies (Kinesis, requests) mocked.
    """
    # --- THIS IS THE CORRECTED PATCH TARGET ---
    # We patch 'config' within the module where it is imported and used.
    mocker.patch('src.producer.config', {
        'wikimedia': {'stream_url': 'http://test.stream.url'},
        'aws': {
            'kinesis_stream_name': 'test-stream',
            'region': 'test-region-1'
        }
    })

    # Mock the boto3 client so no real AWS calls are made
    mock_kinesis_client = mocker.MagicMock()
    mocker.patch('boto3.client', return_value=mock_kinesis_client)

    # Instantiate the producer. The __init__ will now use our mocked config and client.
    producer = WikimediaProducer()
    return producer


def test_send_to_kinesis_method(mock_producer):
    """
    Tests the internal _send_to_kinesis method to ensure it calls
    the boto3 client with the correct parameters.
    """
    # The sample record to be sent
    test_record = {"meta": {"domain": "en.wikipedia.org"}, "id": 123, "type": "edit"}

    # Call the method we are testing
    mock_producer._send_to_kinesis(test_record)

    # Assert that the put_record method was called exactly once
    mock_producer.kinesis_client.put_record.assert_called_once()

    # Get the arguments that put_record was called with
    _, call_kwargs = mock_producer.kinesis_client.put_record.call_args

    # Assert the parameters are correct
    assert call_kwargs['StreamName'] == 'test-stream'
    assert call_kwargs['PartitionKey'] == "en.wikipedia.org"
    assert call_kwargs['Data'] == json.dumps(test_record).encode('utf-8')


def test_run_loop_logic(mocker, mock_producer):
    """
    Tests the main run() method's loop to ensure it handles valid and invalid lines.
    """
    # Mock the response from the requests session
    mock_response = mocker.MagicMock()
    mock_response.iter_lines.return_value = [SAMPLE_SSE_EVENT, SAMPLE_KEEP_ALIVE]

    # --- THIS IS THE CORRECTED MOCKING PATTERN FOR A CONTEXT MANAGER ---
    # The session's `get` method returns a context manager, so we mock it to do the same.
    mock_producer.session.get = mocker.MagicMock()
    mock_producer.session.get.return_value.__enter__.return_value = mock_response

    # Spy on the internal _send_to_kinesis method to see if it gets called
    send_spy = mocker.spy(mock_producer, '_send_to_kinesis')

    # Run the main loop
    mock_producer.run()

    # Assert that _send_to_kinesis was called exactly once, only for the valid data
    send_spy.assert_called_once()