"""Config loader sử dụng Pydantic Settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

from ..domain.value_objects.processing_config import ProcessingConfig


class AppSettings(BaseSettings):
    """Application settings sử dụng Pydantic Settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Kafka source configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "stream-events"
    kafka_group_id: str = "spark-stream-group"
    kafka_value_format: str = "json"  # json, avro, protobuf
    kafka_schema_registry_url: Optional[str] = None
    
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


class ConfigLoader:
    """Load configuration từ environment variables sử dụng Pydantic Settings."""
    
    @staticmethod
    def load_from_env() -> ProcessingConfig:
        """Load config từ environment variables.
        
        Returns:
            ProcessingConfig: Processing configuration object
        """
        settings = AppSettings()
        
        return ProcessingConfig(
            # Kafka source configuration
            kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
            kafka_topic=settings.kafka_topic,
            kafka_group_id=settings.kafka_group_id,
            kafka_value_format=settings.kafka_value_format,
            kafka_schema_registry_url=settings.kafka_schema_registry_url,
            
            # ClickHouse sink configuration
            clickhouse_host=settings.clickhouse_host,
            clickhouse_port=settings.clickhouse_port,
            clickhouse_database=settings.clickhouse_database,
            clickhouse_table=settings.clickhouse_table,
            clickhouse_user=settings.clickhouse_user,
            clickhouse_password=settings.clickhouse_password,
            
            # PostgreSQL sink configuration
            postgres_host=settings.postgres_host,
            postgres_port=settings.postgres_port,
            postgres_database=settings.postgres_database,
            postgres_table=settings.postgres_table,
            postgres_user=settings.postgres_user,
            postgres_password=settings.postgres_password,
            
            # Iceberg sink configuration
            iceberg_database=settings.iceberg_database,
            iceberg_table=settings.iceberg_table,
            iceberg_catalog=settings.iceberg_catalog,
            iceberg_warehouse=settings.iceberg_warehouse,
            
            # Spark configuration
            batch_interval_seconds=settings.batch_interval_seconds,
            checkpoint_location=settings.checkpoint_location
        )
