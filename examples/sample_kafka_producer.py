"""Ví dụ script để gửi sample events vào Kafka topic."""
import json
import time
from datetime import datetime
from kafka import KafkaProducer


def create_sample_event(event_id: str, event_type: str) -> dict:
    """Tạo sample event."""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "payload": {
            "user_id": f"user_{event_id}",
            "action": "click",
            "page": "/home",
            "value": 100
        },
        "source": "web-app",
        "metadata": {
            "version": "1.0",
            "environment": "production"
        }
    }


def main():
    """Gửi sample events vào Kafka."""
    # Cấu hình Kafka
    bootstrap_servers = "localhost:9092"
    topic = "stream-events"
    
    # Tạo producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Đang gửi events vào topic: {topic}")
    
    # Gửi 10 sample events
    for i in range(10):
        event = create_sample_event(
            event_id=f"event_{i}_{int(time.time())}",
            event_type="user_action"
        )
        
        producer.send(topic, event)
        print(f"Đã gửi event {i+1}: {event['event_id']}")
        time.sleep(1)
    
    producer.flush()
    producer.close()
    print("Đã gửi xong tất cả events")


if __name__ == "__main__":
    main()

