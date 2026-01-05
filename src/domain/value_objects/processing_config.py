"""Processing configuration value object."""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ProcessingConfig:
    """Value object chứa cấu hình xử lý stream."""
    
    # Kafka source configuration
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    kafka_value_format: str = "json"  # json, avro, protobuf
    kafka_schema_registry_url: Optional[str] = None  # Required for avro/protobuf
    
    # ClickHouse sink configuration
    clickhouse_host: Optional[str] = None
    clickhouse_port: Optional[int] = None
    clickhouse_database: Optional[str] = None
    clickhouse_table: Optional[str] = None
    clickhouse_user: Optional[str] = None
    clickhouse_password: Optional[str] = None
    
    # PostgreSQL sink configuration
    postgres_host: Optional[str] = None
    postgres_port: Optional[int] = None
    postgres_database: Optional[str] = None
    postgres_table: Optional[str] = None
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    
    # Iceberg sink configuration
    iceberg_database: Optional[str] = None
    iceberg_table: Optional[str] = None
    iceberg_catalog: Optional[str] = None
    iceberg_warehouse: Optional[str] = None
    
    # Spark configuration
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
        
        # Validate ít nhất một sink được cấu hình
        has_clickhouse = all([
            self.clickhouse_host,
            self.clickhouse_port,
            self.clickhouse_database,
            self.clickhouse_table
        ])
        has_postgres = all([
            self.postgres_host,
            self.postgres_port,
            self.postgres_database,
            self.postgres_table
        ])
        has_iceberg = all([
            self.iceberg_database,
            self.iceberg_table
        ])
        
        if not (has_clickhouse or has_postgres or has_iceberg):
            raise ValueError("Phải cấu hình ít nhất một sink (ClickHouse, PostgreSQL, hoặc Iceberg)")
        
        # Validate ClickHouse nếu được cấu hình
        if has_clickhouse:
            if self.clickhouse_port <= 0:
                raise ValueError("clickhouse_port phải lớn hơn 0")
        
        # Validate PostgreSQL nếu được cấu hình
        if has_postgres:
            if self.postgres_port <= 0:
                raise ValueError("postgres_port phải lớn hơn 0")

