"""Kafka stream reader implementation."""
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from ...domain.value_objects.processing_config import ProcessingConfig
from ...application.use_cases.process_stream import StreamReader


class KafkaStreamReader(StreamReader):
    """Implementation của StreamReader sử dụng Kafka."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
    
    def read_stream(self, config: ProcessingConfig) -> DataFrame:
        """Đọc stream từ Kafka topic."""
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
        
        # Parse JSON từ value column
        from pyspark.sql.functions import col, from_json
        from pyspark.sql.types import StructType, StringType
        
        # Schema cho event data - payload và metadata là String để giữ nguyên JSON string
        # Sẽ parse sau trong use case
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

