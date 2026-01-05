"""Use case để xử lý stream từ source và ghi vào sinks."""
from typing import List

from ...domain.entities.stream_event import StreamEvent
from ...domain.services.event_processor import EventProcessor
from ...domain.ports.source_connector import SourceConnector
from ...domain.ports.sink_connector import SinkConnector
from ...domain.value_objects.processing_config import ProcessingConfig


class ProcessStreamUseCase:
    """Use case xử lý stream từ source và ghi vào sinks."""
    
    def __init__(
        self,
        source_connector: SourceConnector,
        sink_connectors: List[SinkConnector],
        event_processor: EventProcessor,
        config: ProcessingConfig
    ):
        """Khởi tạo use case.
        
        Args:
            source_connector: Source connector để đọc stream
            sink_connectors: Danh sách sink connectors để ghi dữ liệu
            event_processor: Event processor để xử lý events
            config: ProcessingConfig
        """
        self.source_connector = source_connector
        self.sink_connectors = sink_connectors
        self.event_processor = event_processor
        self.config = config
    
    def execute(self) -> None:
        """Thực thi use case."""
        # Đọc stream từ source
        stream_df = self.source_connector.read_stream(self.config)
        
        # Xử lý và ghi vào sinks
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
            
            # Ghi vào tất cả các sinks đã cấu hình
            if processed_events:
                for sink_connector in self.sink_connectors:
                    try:
                        sink_connector.write_events(processed_events)
                    except Exception as e:
                        print(f"Lỗi khi ghi vào sink {type(sink_connector).__name__}: {e}")
                        import traceback
                        traceback.print_exc()
                        # Tiếp tục với các sink khác
                        continue
        
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
        try:
            query.awaitTermination()
        finally:
            # Đóng tất cả connectors khi kết thúc
            self.source_connector.close()
            for sink_connector in self.sink_connectors:
                sink_connector.close()

