"""Main entry point cho Spark Stream Processing application."""
import sys
import signal
from typing import Optional

from src.presentation.config_loader import ConfigLoader
from src.infrastructure.connectors.spark_session_factory import SparkSessionFactory
from src.infrastructure.connectors.connector_factory import ConnectorFactory
from src.domain.services.event_processor import EventProcessor
from src.application.use_cases.process_stream import ProcessStreamUseCase
from src.domain.value_objects.processing_config import ProcessingConfig


class StreamProcessingApp:
    """Application class để quản lý lifecycle của stream processing."""
    
    def __init__(self):
        self.config: Optional[ProcessingConfig] = None
        self.spark_session = None
        self.source_connector = None
        self.sink_connectors = None
        self.event_processor = None
        self.use_case = None
    
    def setup(self):
        """Khởi tạo các dependencies."""
        print("Đang khởi tạo Spark Stream Processing...")
        
        # Load config
        self.config = ConfigLoader.load_from_env()
        print(f"Config đã được load: Kafka topic={self.config.kafka_topic}")
        
        # Tạo Spark session
        self.spark_session = SparkSessionFactory.create_session(
            app_name="SparkStreamProcessing",
            checkpoint_location=self.config.checkpoint_location
        )
        print("Spark session đã được tạo")
        
        # Tạo source connector sử dụng factory
        self.source_connector = ConnectorFactory.create_source_connector(
            self.spark_session,
            self.config
        )
        print(f"Source connector ({type(self.source_connector).__name__}) đã được khởi tạo")
        
        # Tạo sink connectors sử dụng factory
        self.sink_connectors = ConnectorFactory.create_sink_connectors(
            self.spark_session,
            self.config
        )
        sink_names = [type(c).__name__ for c in self.sink_connectors]
        print(f"Sink connectors ({', '.join(sink_names)}) đã được khởi tạo")
        
        # Tạo event processor
        self.event_processor = EventProcessor()
        print("Event processor đã được khởi tạo")
        
        # Tạo use case với DI
        self.use_case = ProcessStreamUseCase(
            source_connector=self.source_connector,
            sink_connectors=self.sink_connectors,
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
        
        # Đóng source connector
        if self.source_connector:
            try:
                self.source_connector.close()
            except Exception as e:
                print(f"Lỗi khi đóng source connector: {e}")
        
        # Đóng tất cả sink connectors
        if self.sink_connectors:
            for sink_connector in self.sink_connectors:
                try:
                    sink_connector.close()
                except Exception as e:
                    print(f"Lỗi khi đóng sink connector {type(sink_connector).__name__}: {e}")
        
        # Dừng Spark session
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
