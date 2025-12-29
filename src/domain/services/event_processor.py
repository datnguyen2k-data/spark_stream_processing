"""Domain service để xử lý events."""
from typing import List
from datetime import datetime

from ..entities.stream_event import StreamEvent


class EventProcessor:
    """Domain service xử lý business logic cho events."""
    
    def validate_event(self, event: StreamEvent) -> bool:
        """Validate event theo business rules."""
        # Kiểm tra timestamp không được trong tương lai
        if event.timestamp > datetime.now():
            return False
        
        # Kiểm tra payload không được rỗng
        if not event.payload:
            return False
        
        return True
    
    def enrich_event(self, event: StreamEvent) -> StreamEvent:
        """Làm giàu event với thông tin bổ sung."""
        enriched_metadata = event.metadata or {}
        enriched_metadata.update({
            "processed_at": datetime.now().isoformat(),
            "processing_version": "1.0"
        })
        
        return StreamEvent(
            event_id=event.event_id,
            event_type=event.event_type,
            timestamp=event.timestamp,
            payload=event.payload,
            source=event.source,
            metadata=enriched_metadata
        )
    
    def transform_event(self, event: StreamEvent) -> StreamEvent:
        """Transform event theo business rules."""
        # Có thể thêm logic transform ở đây
        # Ví dụ: normalize data, calculate fields, etc.
        return event
    
    def process_events(self, events: List[StreamEvent]) -> List[StreamEvent]:
        """Xử lý batch events."""
        processed_events = []
        
        for event in events:
            if not self.validate_event(event):
                continue
            
            enriched_event = self.enrich_event(event)
            transformed_event = self.transform_event(enriched_event)
            processed_events.append(transformed_event)
        
        return processed_events

