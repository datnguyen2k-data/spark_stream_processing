"""Processing configuration value object."""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ProcessingConfig:
    """Value object chứa cấu hình xử lý stream."""
    
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_table: str
    clickhouse_user: Optional[str] = None
    clickhouse_password: Optional[str] = None
    batch_interval_seconds: int = 10
    checkpoint_location: str = "/tmp/spark-checkpoint"
    
    def __post_init__(self):
        """Validate configuration."""
        if not self.kafka_bootstrap_servers:
            raise ValueError("kafka_bootstrap_servers không được để trống")
        if not self.kafka_topic:
            raise ValueError("kafka_topic không được để trống")
        if not self.kafka_group_id:
            raise ValueError("kafka_group_id không được để trống")
        if not self.clickhouse_host:
            raise ValueError("clickhouse_host không được để trống")
        if self.clickhouse_port <= 0:
            raise ValueError("clickhouse_port phải lớn hơn 0")
        if not self.clickhouse_database:
            raise ValueError("clickhouse_database không được để trống")
        if not self.clickhouse_table:
            raise ValueError("clickhouse_table không được để trống")

