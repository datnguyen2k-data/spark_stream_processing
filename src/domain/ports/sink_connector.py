"""Interface cho sink connector."""
from abc import ABC, abstractmethod
from typing import List

from ..entities.stream_event import StreamEvent


class SinkConnector(ABC):
    """Abstract base class cho sink connector."""
    
    @abstractmethod
    def write_events(self, events: List[StreamEvent]) -> None:
        """Ghi events vào sink.
        
        Args:
            events: Danh sách StreamEvent cần ghi
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Đóng kết nối đến sink."""
        pass

