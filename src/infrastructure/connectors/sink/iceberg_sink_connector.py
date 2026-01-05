"""Apache Iceberg sink connector implementation."""
from typing import List

from pyspark.sql import SparkSession

from ....domain.ports.sink_connector import SinkConnector
from ....domain.entities.stream_event import StreamEvent
from ....domain.value_objects.processing_config import ProcessingConfig
from ....domain.schemas.stream_event_schema import StreamEventSchema
from ....domain.services.schema_evolution_service import SchemaEvolutionService
from ....domain.services.dataframe_converter import DataFrameConverter


class IcebergSinkConnector(SinkConnector):
    """Implementation của SinkConnector sử dụng Apache Iceberg."""
    
    def __init__(
        self,
        spark_session: SparkSession,
        config: ProcessingConfig,
        schema: StreamEventSchema = None
    ):
        """Khởi tạo Iceberg sink connector.
        
        Args:
            spark_session: SparkSession instance
            config: ProcessingConfig chứa cấu hình Iceberg
            schema: StreamEventSchema để quản lý schema evolution
        """
        self.spark_session = spark_session
        self.config = config
        self.schema = schema or StreamEventSchema()
        self.schema_service = SchemaEvolutionService(self.schema)
        self._ensure_table_exists()
    
    def _ensure_table_exists(self) -> None:
        """Đảm bảo table tồn tại trong Iceberg với schema evolution."""
        try:
            # Tạo database nếu chưa tồn tại
            self.spark_session.sql(
                f"CREATE DATABASE IF NOT EXISTS {self.config.iceberg_database}"
            )
            
            # Sử dụng schema service để tạo table
            table_path = f"{self.config.iceberg_database}.{self.config.iceberg_table}"
            self.schema_service.ensure_table_exists(
                self.schema,
                'iceberg',
                table_path,
                self.spark_session
            )
                
        except Exception as e:
            print(f"Lỗi khi tạo table: {e}")
            raise
    
    def write_events(self, events: List[StreamEvent]) -> None:
        """Ghi events vào Iceberg.
        
        Args:
            events: Danh sách StreamEvent cần ghi
        """
        if not events:
            return
        
        try:
            # Convert events thành Spark DataFrame sử dụng service
            df = DataFrameConverter.events_to_dataframe(
                self.spark_session,
                events,
                self.schema
            )
            
            # Ghi vào Iceberg table
            table_path = f"{self.config.iceberg_database}.{self.config.iceberg_table}"
            
            # Sử dụng write format iceberg với mode append
            # Lưu ý: Cần cấu hình Spark với Iceberg packages
            df.write \
                .format("iceberg") \
                .mode("append") \
                .option("path", table_path) \
                .saveAsTable(table_path)
            
            print(f"Đã ghi {len(events)} events vào Iceberg")
            
        except Exception as e:
            print(f"Lỗi khi ghi vào Iceberg: {e}")
            raise
    
    def close(self) -> None:
        """Đóng connection (Spark tự quản lý)."""
        # Spark tự quản lý Iceberg connection, không cần đóng thủ công
        pass

