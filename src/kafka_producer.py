import json
import os
from kafka import KafkaProducer
import requests

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "wikipedia-changes"
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {"User-Agent": "IMDbBigDataProject/1.0 (student@ece.fr)"}
    
    response = requests.get(url, stream=True, headers=headers)
    for line in response.iter_lines():
        if line:
            line_str = line.decode("utf-8")
            if line_str.startswith("data: "):
                data_json = line_str[6:]
                try:
                    event = json.loads(data_json)
                    producer.send(topic, value=event)
                except Exception:
                    pass

if __name__ == "__main__":
    main()