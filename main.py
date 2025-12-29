"""Main entry point cho Spark Stream Processing application."""
import sys
import signal
from typing import Optional

from src.presentation.config_loader import ConfigLoader
from src.infrastructure.spark.spark_session_factory import SparkSessionFactory
from src.infrastructure.kafka.kafka_stream_reader import KafkaStreamReader
from src.infrastructure.clickhouse.clickhouse_writer import ClickHouseWriter
from src.domain.services.event_processor import EventProcessor
from src.application.use_cases.process_stream import ProcessStreamUseCase


class StreamProcessingApp:
    """Application class để quản lý lifecycle của stream processing."""
    
    def __init__(self):
        self.config: Optional[ConfigLoader] = None
        self.spark_session = None
        self.stream_reader = None
        self.stream_writer = None
        self.event_processor = None
        self.use_case = None
    
    def setup(self):
        """Khởi tạo các dependencies."""
        print("Đang khởi tạo Spark Stream Processing...")
        
        # Load config
        self.config = ConfigLoader.load_from_env()
        print(f"Config đã được load: Kafka topic={self.config.kafka_topic}, "
              f"ClickHouse table={self.config.clickhouse_table}")
        
        # Tạo Spark session
        self.spark_session = SparkSessionFactory.create_session(
            app_name="SparkStreamProcessing",
            checkpoint_location=self.config.checkpoint_location
        )
        print("Spark session đã được tạo")
        
        # Tạo stream reader
        self.stream_reader = KafkaStreamReader(self.spark_session)
        print("Kafka stream reader đã được khởi tạo")
        
        # Tạo stream writer
        self.stream_writer = ClickHouseWriter(self.config)
        print("ClickHouse writer đã được khởi tạo")
        
        # Tạo event processor
        self.event_processor = EventProcessor()
        print("Event processor đã được khởi tạo")
        
        # Tạo use case
        self.use_case = ProcessStreamUseCase(
            stream_reader=self.stream_reader,
            stream_writer=self.stream_writer,
            event_processor=self.event_processor,
            config=self.config
        )
        print("Use case đã được khởi tạo")
    
    def run(self):
        """Chạy stream processing."""
        try:
            print("Bắt đầu xử lý stream...")
            self.use_case.execute()
        except KeyboardInterrupt:
            print("\nĐang dừng stream processing...")
            self.shutdown()
        except Exception as e:
            print(f"Lỗi khi xử lý stream: {e}")
            self.shutdown()
            sys.exit(1)
    
    def shutdown(self):
        """Dọn dẹp resources."""
        print("Đang dọn dẹp resources...")
        
        if self.stream_writer:
            self.stream_writer.close()
        
        if self.spark_session:
            SparkSessionFactory.stop_session()
        
        print("Đã dọn dẹp xong")


def main():
    """Main function."""
    app = StreamProcessingApp()
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        print("\nNhận được signal để dừng...")
        app.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Setup và run
    app.setup()
    app.run()


if __name__ == "__main__":
    main()
