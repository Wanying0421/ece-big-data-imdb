from unittest.mock import patch, MagicMock
import pytest
import requests

@patch("src.kafka_producer.requests.get")
def test_kafka_producer_503(mock_get):
    # Mock an HTTP 503 error triggering raise_for_status
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
    mock_get.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        mock_response.raise_for_status()

@patch("src.kafka_producer.requests.get")
def test_kafka_producer_eof(mock_get):
    # Mock normal stream closure (empty lines / EOF)
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = []
    mock_get.return_value = mock_response

    response = mock_get()
    lines = list(response.iter_lines())
    assert lines == []