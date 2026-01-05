"""Factory để tạo Spark session."""
from pyspark.sql import SparkSession
from typing import Optional


class SparkSessionFactory:
    """Factory để tạo và quản lý Spark session."""
    
    _spark_session: Optional[SparkSession] = None
    
    @classmethod
    def create_session(
        cls,
        app_name: str = "SparkStreamProcessing",
        master: str = "local[*]",
        checkpoint_location: str = "/tmp/spark-checkpoint"
    ) -> SparkSession:
        """Tạo Spark session với cấu hình phù hợp cho streaming."""
        if cls._spark_session is None:
            cls._spark_session = (
                SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()
            )
            
            # Set log level
            cls._spark_session.sparkContext.setLogLevel("WARN")
        
        return cls._spark_session
    
    @classmethod
    def get_session(cls) -> Optional[SparkSession]:
        """Lấy Spark session hiện tại."""
        return cls._spark_session
    
    @classmethod
    def stop_session(cls) -> None:
        """Dừng Spark session."""
        if cls._spark_session is not None:
            cls._spark_session.stop()
            cls._spark_session = None

