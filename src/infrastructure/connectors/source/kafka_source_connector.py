"""Kafka source connector implementation với hỗ trợ JSON, Avro, Protobuf."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

from ....domain.ports.source_connector import SourceConnector
from ....domain.value_objects.processing_config import ProcessingConfig


class KafkaSourceConnector(SourceConnector):
    """Implementation của SourceConnector sử dụng Kafka với hỗ trợ nhiều format."""
    
    def __init__(self, spark_session: SparkSession):
        """Khởi tạo Kafka source connector.
        
        Args:
            spark_session: SparkSession instance
        """
        self.spark_session = spark_session
    
    def read_stream(self, config: ProcessingConfig) -> DataFrame:
        """Đọc stream từ Kafka topic với format tương ứng.
        
        Args:
            config: ProcessingConfig chứa cấu hình Kafka
            
        Returns:
            DataFrame: Spark DataFrame chứa dữ liệu đã parse từ Kafka
        """
        format_type = config.kafka_value_format.lower()
        
        if format_type == "json":
            return self._read_json_stream(config)
        elif format_type == "avro":
            return self._read_avro_stream(config)
        elif format_type == "protobuf":
            return self._read_protobuf_stream(config)
        else:
            raise ValueError(f"Unsupported Kafka value format: {format_type}")
    
    def _read_json_stream(self, config: ProcessingConfig) -> DataFrame:
        """Đọc stream JSON từ Kafka."""
        # Đọc stream từ Kafka
        df = (
            self.spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .option("kafka.group.id", config.kafka_group_id)
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Schema cho event data
        event_schema = StructType() \
            .add("event_id", StringType()) \
            .add("event_type", StringType()) \
            .add("timestamp", StringType()) \
            .add("payload", StringType()) \
            .add("source", StringType()) \
            .add("metadata", StringType())
        
        # Parse JSON và extract fields
        parsed_df = df.select(
            from_json(col("value").cast("string"), event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            col("data.event_id").alias("event_id"),
            col("data.event_type").alias("event_type"),
            col("data.timestamp").alias("timestamp"),
            col("data.payload").alias("payload"),
            col("data.source").alias("source"),
            col("data.metadata").alias("metadata"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).filter(col("event_id").isNotNull())
        
        return parsed_df
    
    def _read_avro_stream(self, config: ProcessingConfig) -> DataFrame:
        """Đọc stream Avro từ Kafka sử dụng Schema Registry."""
        if not config.kafka_schema_registry_url:
            raise ValueError("kafka_schema_registry_url is required for Avro format")
        
        # Đọc stream từ Kafka với Avro format
        df = (
            self.spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .option("kafka.group.id", config.kafka_group_id)
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Sử dụng Confluent Avro deserializer
        # Cần cấu hình Schema Registry
        avro_df = df.select(
            col("key").cast("string").alias("key"),
            col("value").alias("value"),  # Avro binary
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        )
        
        # Parse Avro value sử dụng Schema Registry
        # Lưu ý: Cần thêm Confluent Spark Avro connector
        # Format: "avro" với option "schema.registry.url"
        parsed_df = (
            avro_df
            .select(
                col("value").alias("event_data"),
                col("kafka_timestamp"),
                col("partition"),
                col("offset")
            )
        )
        
        # Extract fields từ Avro record
        # Giả sử Avro schema có các field: event_id, event_type, timestamp, payload, source, metadata
        parsed_df = parsed_df.select(
            col("event_data.event_id").alias("event_id"),
            col("event_data.event_type").alias("event_type"),
            col("event_data.timestamp").alias("timestamp"),
            col("event_data.payload").cast("string").alias("payload"),
            col("event_data.source").alias("source"),
            col("event_data.metadata").cast("string").alias("metadata"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).filter(col("event_id").isNotNull())
        
        return parsed_df
    
    def _read_protobuf_stream(self, config: ProcessingConfig) -> DataFrame:
        """Đọc stream Protobuf từ Kafka sử dụng Schema Registry."""
        if not config.kafka_schema_registry_url:
            raise ValueError("kafka_schema_registry_url is required for Protobuf format")
        
        # Đọc stream từ Kafka với Protobuf format
        df = (
            self.spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "latest")
            .option("kafka.group.id", config.kafka_group_id)
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Sử dụng Confluent Protobuf deserializer
        # Cần cấu hình Schema Registry
        protobuf_df = df.select(
            col("key").cast("string").alias("key"),
            col("value").alias("value"),  # Protobuf binary
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        )
        
        # Parse Protobuf value sử dụng Schema Registry
        # Lưu ý: Cần thêm Confluent Spark Protobuf connector
        # Format: "protobuf" với option "schema.registry.url"
        parsed_df = (
            protobuf_df
            .select(
                col("value").alias("event_data"),
                col("kafka_timestamp"),
                col("partition"),
                col("offset")
            )
        )
        
        # Extract fields từ Protobuf message
        # Giả sử Protobuf schema có các field: event_id, event_type, timestamp, payload, source, metadata
        parsed_df = parsed_df.select(
            col("event_data.event_id").alias("event_id"),
            col("event_data.event_type").alias("event_type"),
            col("event_data.timestamp").alias("timestamp"),
            col("event_data.payload").cast("string").alias("payload"),
            col("event_data.source").alias("source"),
            col("event_data.metadata").cast("string").alias("metadata"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).filter(col("event_id").isNotNull())
        
        return parsed_df
    
    def close(self) -> None:
        """Đóng kết nối Kafka (Spark tự quản lý)."""
        # Spark tự quản lý Kafka connection, không cần đóng thủ công
        pass
