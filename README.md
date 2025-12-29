# Spark Stream Processing

Dự án xử lý stream dữ liệu sử dụng Apache Spark Streaming với kiến trúc Domain-Driven Design (DDD).

## Kiến trúc

Dự án được tổ chức theo kiến trúc DDD với các layer sau:

```
src/
├── domain/              # Domain layer - Business logic
│   ├── entities/        # Domain entities
│   ├── value_objects/   # Value objects
│   └── services/        # Domain services
├── application/         # Application layer - Use cases
│   └── use_cases/       # Application use cases
├── infrastructure/      # Infrastructure layer - External services
│   ├── spark/           # Spark configuration
│   ├── kafka/           # Kafka integration
│   └── clickhouse/      # ClickHouse integration
└── presentation/        # Presentation layer - Entry point
    └── config_loader.py # Configuration loader
```

## Tính năng

- ✅ Đọc dữ liệu từ Kafka topic sử dụng Spark Streaming
- ✅ Xử lý dữ liệu theo batch với domain services
- ✅ Ghi dữ liệu đã xử lý vào ClickHouse
- ✅ Kiến trúc DDD với separation of concerns rõ ràng
- ✅ Dễ dàng test và maintain

## Yêu cầu

- Python >= 3.12
- Apache Spark >= 3.5.0
- Kafka (để cung cấp dữ liệu stream)
- ClickHouse (để lưu trữ dữ liệu đã xử lý)

## Cài đặt

1. Clone repository:
```bash
git clone <repository-url>
cd spark_stream_processing
```

2. Tạo virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Trên Windows: .venv\Scripts\activate
```

3. Cài đặt dependencies:
```bash
pip install -r requirements.txt
```

4. Cấu hình environment variables:
```bash
cp .env.example .env
# Chỉnh sửa .env với thông tin Kafka và ClickHouse của bạn
```

## Cấu hình

Tạo file `.env` với các biến môi trường sau:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=stream-events
KAFKA_GROUP_ID=spark-stream-group

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=default
CLICKHOUSE_TABLE=stream_events
CLICKHOUSE_USER=
CLICKHOUSE_PASSWORD=

# Spark Configuration
BATCH_INTERVAL_SECONDS=10
CHECKPOINT_LOCATION=/tmp/spark-checkpoint
```

## Chạy ứng dụng

1. Đảm bảo Kafka và ClickHouse đang chạy
2. Cấu hình file `.env` với thông tin kết nối
3. Chạy ứng dụng:

```bash
python main.py
```

## Testing với sample data

Để test ứng dụng với sample data, bạn có thể sử dụng script example:

```bash
# Cài đặt kafka-python (chỉ cần cho example)
pip install kafka-python

# Chạy producer để gửi sample events
python examples/sample_kafka_producer.py
```

Script này sẽ gửi 10 sample events vào Kafka topic `stream-events`.

## Cấu trúc dữ liệu

### Input (Kafka message)

Message từ Kafka phải có format JSON:

```json
{
  "event_id": "unique-event-id",
  "event_type": "user_action",
  "timestamp": "2024-01-01T00:00:00",
  "payload": {
    "user_id": "123",
    "action": "click"
  },
  "source": "web-app",
  "metadata": {
    "version": "1.0"
  }
}
```

### Output (ClickHouse table)

Dữ liệu được ghi vào ClickHouse table với schema:

```sql
CREATE TABLE stream_events (
    event_id String,
    event_type String,
    timestamp DateTime,
    payload String,
    source String,
    metadata String,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (timestamp, event_id)
PARTITION BY toYYYYMM(timestamp)
```

## Development

### Cấu trúc DDD

- **Domain Layer**: Chứa business logic, entities, value objects và domain services
- **Application Layer**: Chứa use cases và orchestration logic
- **Infrastructure Layer**: Implementations cho external services (Kafka, ClickHouse, Spark)
- **Presentation Layer**: Entry point và configuration

### Thêm domain logic mới

1. Thêm entities trong `src/domain/entities/`
2. Thêm value objects trong `src/domain/value_objects/`
3. Thêm domain services trong `src/domain/services/`
4. Tạo use cases trong `src/application/use_cases/`

## License

MIT

