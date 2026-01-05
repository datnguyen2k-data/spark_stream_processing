"""Factory để tạo connectors dựa trên configuration."""
from typing import List, Optional

from pyspark.sql import SparkSession

from ...domain.ports.source_connector import SourceConnector
from ...domain.ports.sink_connector import SinkConnector
from ...domain.value_objects.processing_config import ProcessingConfig

from .source.kafka_source_connector import KafkaSourceConnector
from .sink.clickhouse_sink_connector import ClickHouseSinkConnector
from .sink.postgres_sink_connector import PostgresSinkConnector
from .sink.iceberg_sink_connector import IcebergSinkConnector
from ...domain.schemas.stream_event_schema import StreamEventSchema


class ConnectorFactory:
    """Factory để tạo source và sink connectors."""
    
    @staticmethod
    def create_source_connector(
        spark_session: SparkSession,
        config: ProcessingConfig
    ) -> SourceConnector:
        """Tạo source connector dựa trên configuration.
        
        Args:
            spark_session: SparkSession instance
            config: ProcessingConfig
            
        Returns:
            SourceConnector: Source connector instance
        """
        # Hiện tại chỉ hỗ trợ Kafka
        return KafkaSourceConnector(spark_session)
    
    @staticmethod
    def create_sink_connectors(
        spark_session: SparkSession,
        config: ProcessingConfig,
        schema: StreamEventSchema = None
    ) -> List[SinkConnector]:
        """Tạo danh sách sink connectors dựa trên configuration.
        
        Args:
            spark_session: SparkSession instance
            config: ProcessingConfig
            schema: StreamEventSchema để quản lý schema evolution
            
        Returns:
            List[SinkConnector]: Danh sách sink connectors
        """
        connectors = []
        schema = schema or StreamEventSchema()
        
        # Tạo ClickHouse connector nếu được cấu hình
        if all([
            config.clickhouse_host,
            config.clickhouse_port,
            config.clickhouse_database,
            config.clickhouse_table
        ]):
            connectors.append(ClickHouseSinkConnector(spark_session, config, schema))
        
        # Tạo PostgreSQL connector nếu được cấu hình
        if all([
            config.postgres_host,
            config.postgres_port,
            config.postgres_database,
            config.postgres_table
        ]):
            connectors.append(PostgresSinkConnector(spark_session, config, schema))
        
        # Tạo Iceberg connector nếu được cấu hình
        if all([
            config.iceberg_database,
            config.iceberg_table
        ]):
            connectors.append(IcebergSinkConnector(spark_session, config, schema))
        
        if not connectors:
            raise ValueError("Phải cấu hình ít nhất một sink connector")
        
        return connectors

