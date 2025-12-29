"""ClickHouse writer implementation."""
from typing import List
import json
from datetime import datetime

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from ...domain.entities.stream_event import StreamEvent
from ...domain.value_objects.processing_config import ProcessingConfig
from ...application.use_cases.process_stream import StreamWriter


class ClickHouseWriter(StreamWriter):
    """Implementation của StreamWriter sử dụng ClickHouse."""
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.client = self._create_client()
        self._ensure_table_exists()
    
    def _create_client(self) -> Client:
        """Tạo ClickHouse client."""
        return Client(
            host=self.config.clickhouse_host,
            port=self.config.clickhouse_port,
            database=self.config.clickhouse_database,
            user=self.config.clickhouse_user,
            password=self.config.clickhouse_password
        )
    
    def _ensure_table_exists(self) -> None:
        """Đảm bảo table tồn tại trong ClickHouse."""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.clickhouse_table} (
            event_id String,
            event_type String,
            timestamp DateTime,
            payload String,
            source String,
            metadata String,
            processed_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, event_id)
        PARTITION BY toYYYYMM(timestamp)
        """
        
        try:
            self.client.execute(create_table_query)
            print(f"Table {self.config.clickhouse_table} đã được tạo hoặc đã tồn tại")
        except ClickHouseError as e:
            print(f"Lỗi khi tạo table: {e}")
            raise
    
    def write_events(self, events: List[StreamEvent]) -> None:
        """Ghi events vào ClickHouse."""
        if not events:
            return
        
        try:
            # Chuẩn bị data để insert
            data = []
            for event in events:
                # Parse timestamp
                if isinstance(event.timestamp, str):
                    timestamp = datetime.fromisoformat(event.timestamp)
                else:
                    timestamp = event.timestamp
                
                data.append((
                    event.event_id,
                    event.event_type,
                    timestamp,
                    json.dumps(event.payload),
                    event.source,
                    json.dumps(event.metadata or {})
                ))
            
            # Insert batch vào ClickHouse
            insert_query = f"""
            INSERT INTO {self.config.clickhouse_table} 
            (event_id, event_type, timestamp, payload, source, metadata)
            VALUES
            """
            
            self.client.execute(insert_query, data)
            print(f"Đã ghi {len(events)} events vào ClickHouse")
            
        except ClickHouseError as e:
            print(f"Lỗi khi ghi vào ClickHouse: {e}")
            raise
        except Exception as e:
            print(f"Lỗi không mong đợi: {e}")
            raise
    
    def close(self) -> None:
        """Đóng connection."""
        if self.client:
            self.client.disconnect()

