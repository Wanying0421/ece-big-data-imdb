from unittest.mock import patch, MagicMock
import pytest
import requests
from src.kafka_producer import main

@patch("src.kafka_producer.requests.get")
@patch("src.kafka_producer.KafkaProducer")
@patch("src.kafka_producer.time.sleep", side_effect=InterruptedError) # Pour stopper la boucle infinie du test
def test_producer_reconnect_on_error(mock_sleep, mock_kafka, mock_get):
    # Simuler une erreur HTTP 503
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
    mock_get.return_value = mock_response

    # Exécuter et vérifier que l'erreur déclenche bien la pause de reconnexion
    with pytest.raises(InterruptedError):
        main()
    
    mock_response.raise_for_status.assert_called_once()
    assert mock_response.close.called