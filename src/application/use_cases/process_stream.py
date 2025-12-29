"""Use case để xử lý stream từ Kafka và ghi vào ClickHouse."""
from typing import Protocol

from ...domain.entities.stream_event import StreamEvent
from ...domain.services.event_processor import EventProcessor
from ...domain.value_objects.processing_config import ProcessingConfig


class StreamReader(Protocol):
    """Protocol cho stream reader."""
    def read_stream(self, config: ProcessingConfig):
        """Đọc stream từ Kafka."""
        ...


class StreamWriter(Protocol):
    """Protocol cho stream writer."""
    def write_events(self, events: list[StreamEvent]) -> None:
        """Ghi events vào ClickHouse."""
        ...


class ProcessStreamUseCase:
    """Use case xử lý stream từ Kafka và ghi vào ClickHouse."""
    
    def __init__(
        self,
        stream_reader: StreamReader,
        stream_writer: StreamWriter,
        event_processor: EventProcessor,
        config: ProcessingConfig
    ):
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.event_processor = event_processor
        self.config = config
    
    def execute(self) -> None:
        """Thực thi use case."""
        # Đọc stream từ Kafka
        stream_df = self.stream_reader.read_stream(self.config)
        
        # Xử lý và ghi vào ClickHouse
        def process_batch(batch_df, batch_id):
            """Xử lý từng batch."""
            # Convert Spark DataFrame thành list of events
            import json
            events = []
            for row in batch_df.collect():
                try:
                    event_data = row.asDict()
                    
                    # Parse payload từ JSON string
                    if event_data.get("payload"):
                        if isinstance(event_data["payload"], str):
                            event_data["payload"] = json.loads(event_data["payload"])
                        elif not isinstance(event_data["payload"], dict):
                            event_data["payload"] = {}
                    else:
                        event_data["payload"] = {}
                    
                    # Parse metadata từ JSON string
                    if event_data.get("metadata"):
                        if isinstance(event_data["metadata"], str):
                            event_data["metadata"] = json.loads(event_data["metadata"])
                        elif not isinstance(event_data["metadata"], dict):
                            event_data["metadata"] = None
                    else:
                        event_data["metadata"] = None
                    
                    event = StreamEvent.from_dict(event_data)
                    events.append(event)
                except Exception as e:
                    print(f"Lỗi khi parse event: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # Xử lý events qua domain service
            processed_events = self.event_processor.process_events(events)
            
            # Ghi vào ClickHouse
            if processed_events:
                self.stream_writer.write_events(processed_events)
        
        # Áp dụng foreachBatch để xử lý từng batch
        query = (
            stream_df
            .writeStream
            .foreachBatch(process_batch)
            .trigger(processingTime=f"{self.config.batch_interval_seconds} seconds")
            .option("checkpointLocation", self.config.checkpoint_location)
            .start()
        )
        
        # Chờ streaming query
        query.awaitTermination()

