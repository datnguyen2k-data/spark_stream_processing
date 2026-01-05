"""ClickHouse sink connector implementation sử dụng ClickHouse Spark connector."""
from typing import List

from pyspark.sql import SparkSession

from ....domain.ports.sink_connector import SinkConnector
from ....domain.entities.stream_event import StreamEvent
from ....domain.value_objects.processing_config import ProcessingConfig
from ....domain.schemas.stream_event_schema import StreamEventSchema
from ....domain.services.schema_evolution_service import SchemaEvolutionService
from ....domain.services.dataframe_converter import DataFrameConverter


class ClickHouseSinkConnector(SinkConnector):
    """Implementation của SinkConnector sử dụng ClickHouse qua Spark connector."""
    
    def __init__(
        self,
        spark_session: SparkSession,
        config: ProcessingConfig,
        schema: StreamEventSchema = None
    ):
        """Khởi tạo ClickHouse sink connector.
        
        Args:
            spark_session: SparkSession instance
            config: ProcessingConfig chứa cấu hình ClickHouse
            schema: StreamEventSchema để quản lý schema evolution
        """
        self.spark_session = spark_session
        self.config = config
        self.schema = schema or StreamEventSchema()
        self.schema_service = SchemaEvolutionService(self.schema)
        self._ensure_table_exists()
    
    def _get_clickhouse_url(self) -> str:
        """Tạo ClickHouse URL."""
        return f"clickhouse://{self.config.clickhouse_host}:{self.config.clickhouse_port}/{self.config.clickhouse_database}"
    
    def _ensure_table_exists(self) -> None:
        """Đảm bảo table tồn tại trong ClickHouse với schema evolution."""
        table_name = f"{self.config.clickhouse_database}.{self.config.clickhouse_table}"
        self.schema_service.ensure_table_exists(
            self.schema,
            'clickhouse',
            table_name,
            self.spark_session
        )
    
    def write_events(self, events: List[StreamEvent]) -> None:
        """Ghi events vào ClickHouse sử dụng ClickHouse Spark connector.
        
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
            
            # Ghi vào ClickHouse sử dụng ClickHouse Spark connector
            clickhouse_url = self._get_clickhouse_url()
            
            # Sử dụng ClickHouse format
            df.write \
                .format("clickhouse") \
                .mode("append") \
                .option("clickhouse.host", self.config.clickhouse_host) \
                .option("clickhouse.port", str(self.config.clickhouse_port)) \
                .option("clickhouse.database", self.config.clickhouse_database) \
                .option("clickhouse.table", self.config.clickhouse_table) \
                .option("clickhouse.user", self.config.clickhouse_user or "default") \
                .option("clickhouse.password", self.config.clickhouse_password or "") \
                .save()
            
            print(f"Đã ghi {len(events)} events vào ClickHouse")
            
        except Exception as e:
            print(f"Lỗi khi ghi vào ClickHouse: {e}")
            raise
    
    def close(self) -> None:
        """Đóng connection (Spark tự quản lý ClickHouse connection)."""
        # Spark tự quản lý ClickHouse connection, không cần đóng thủ công
        pass
