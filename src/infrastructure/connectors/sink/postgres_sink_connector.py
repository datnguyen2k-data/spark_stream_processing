"""PostgreSQL sink connector implementation sử dụng Spark JDBC."""
from typing import List

from pyspark.sql import SparkSession

from ....domain.ports.sink_connector import SinkConnector
from ....domain.entities.stream_event import StreamEvent
from ....domain.value_objects.processing_config import ProcessingConfig
from ....domain.schemas.stream_event_schema import StreamEventSchema
from ....domain.services.schema_evolution_service import SchemaEvolutionService
from ....domain.services.dataframe_converter import DataFrameConverter


class PostgresSinkConnector(SinkConnector):
    """Implementation của SinkConnector sử dụng PostgreSQL qua Spark JDBC."""
    
    def __init__(
        self,
        spark_session: SparkSession,
        config: ProcessingConfig,
        schema: StreamEventSchema = None
    ):
        """Khởi tạo PostgreSQL sink connector.
        
        Args:
            spark_session: SparkSession instance
            config: ProcessingConfig chứa cấu hình PostgreSQL
            schema: StreamEventSchema để quản lý schema evolution
        """
        self.spark_session = spark_session
        self.config = config
        self.schema = schema or StreamEventSchema()
        self.schema_service = SchemaEvolutionService(self.schema)
        self._ensure_table_exists()
    
    def _get_jdbc_url(self) -> str:
        """Tạo JDBC URL cho PostgreSQL."""
        return f"jdbc:postgresql://{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_database}"
    
    def _get_jdbc_properties(self) -> dict:
        """Tạo JDBC properties."""
        properties = {
            "user": self.config.postgres_user or "postgres",
            "password": self.config.postgres_password or "",
            "driver": "org.postgresql.Driver"
        }
        return properties
    
    def _ensure_table_exists(self) -> None:
        """Đảm bảo table tồn tại trong PostgreSQL với schema evolution."""
        # Sử dụng Spark SQL để tạo table
        self.schema_service.ensure_table_exists(
            self.schema,
            'postgres',
            self.config.postgres_table,
            self.spark_session
        )
    
    def write_events(self, events: List[StreamEvent]) -> None:
        """Ghi events vào PostgreSQL sử dụng Spark JDBC.
        
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
            
            # Ghi vào PostgreSQL sử dụng JDBC
            jdbc_url = self._get_jdbc_url()
            jdbc_properties = self._get_jdbc_properties()
            
            # Sử dụng mode append với option để xử lý conflict
            df.write \
                .mode("append") \
                .option("driver", jdbc_properties["driver"]) \
                .jdbc(
                    url=jdbc_url,
                    table=self.config.postgres_table,
                    properties=jdbc_properties
                )
            
            print(f"Đã ghi {len(events)} events vào PostgreSQL")
            
        except Exception as e:
            print(f"Lỗi khi ghi vào PostgreSQL: {e}")
            raise
    
    def close(self) -> None:
        """Đóng connection (Spark tự quản lý JDBC connection)."""
        # Spark tự quản lý JDBC connection, không cần đóng thủ công
        pass
