"""Stream event entity."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class StreamEvent:
    """Entity đại diện cho một event trong stream."""
    
    event_id: str
    event_type: str
    timestamp: datetime
    payload: Dict[str, Any]
    source: str
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate entity sau khi khởi tạo."""
        if not self.event_id:
            raise ValueError("event_id không được để trống")
        if not self.event_type:
            raise ValueError("event_type không được để trống")
        if not self.source:
            raise ValueError("source không được để trống")
    
    def to_dict(self) -> Dict[str, Any]:
        """Chuyển đổi entity thành dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
            "source": self.source,
            "metadata": self.metadata or {},
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamEvent":
        """Tạo entity từ dictionary."""
        timestamp = data.get("timestamp")
        parsed_timestamp = None
        
        if timestamp:
            if isinstance(timestamp, str):
                try:
                    # Thử parse ISO format (Python 3.11+)
                    # Xử lý timezone Z
                    timestamp_str = timestamp.replace('Z', '+00:00')
                    parsed_timestamp = datetime.fromisoformat(timestamp_str)
                except ValueError:
                    # Thử các format phổ biến khác
                    try:
                        # Format: YYYY-MM-DD HH:MM:SS
                        from datetime import datetime as dt
                        parsed_timestamp = dt.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                            # Format: YYYY-MM-DDTHH:MM:SS
                            parsed_timestamp = dt.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
                        except ValueError:
                            # Fallback về datetime hiện tại
                            parsed_timestamp = datetime.now()
            elif isinstance(timestamp, (int, float)):
                parsed_timestamp = datetime.fromtimestamp(timestamp)
            elif isinstance(timestamp, datetime):
                parsed_timestamp = timestamp
        
        if parsed_timestamp is None:
            parsed_timestamp = datetime.now()
        
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            timestamp=parsed_timestamp,
            payload=data.get("payload", {}),
            source=data["source"],
            metadata=data.get("metadata"),
        )

