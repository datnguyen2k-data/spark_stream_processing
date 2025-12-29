"""Config loader từ environment variables."""
import os
from typing import Optional

from ..domain.value_objects.processing_config import ProcessingConfig


class ConfigLoader:
    """Load configuration từ environment variables."""
    
    @staticmethod
    def load_from_env() -> ProcessingConfig:
        """Load config từ environment variables."""
        return ProcessingConfig(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", 
                "localhost:9092"
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", "stream-events"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "spark-stream-group"),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "default"),
            clickhouse_table=os.getenv("CLICKHOUSE_TABLE", "stream_events"),
            clickhouse_user=os.getenv("CLICKHOUSE_USER"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD"),
            batch_interval_seconds=int(os.getenv("BATCH_INTERVAL_SECONDS", "10")),
            checkpoint_location=os.getenv(
                "CHECKPOINT_LOCATION", 
                "/tmp/spark-checkpoint"
            )
        )

