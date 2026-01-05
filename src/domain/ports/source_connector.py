"""Interface cho source connector."""
from abc import ABC, abstractmethod
from typing import Protocol

from pyspark.sql import DataFrame

from ..value_objects.processing_config import ProcessingConfig


class SourceConnector(ABC):
    """Abstract base class cho source connector."""
    
    @abstractmethod
    def read_stream(self, config: ProcessingConfig) -> DataFrame:
        """Đọc stream từ source.
        
        Args:
            config: ProcessingConfig chứa cấu hình kết nối
            
        Returns:
            DataFrame: Spark DataFrame chứa dữ liệu stream
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Đóng kết nối đến source."""
        pass

