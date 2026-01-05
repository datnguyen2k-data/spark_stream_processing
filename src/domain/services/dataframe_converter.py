"""Service để convert StreamEvent list thành Spark DataFrame."""
from typing import List
import json
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from ..entities.stream_event import StreamEvent
from ..schemas.stream_event_schema import StreamEventSchema, FieldType
from .spark_schema_converter import SparkSchemaConverter


class DataFrameConverter:
    """Service để convert StreamEvent list thành Spark DataFrame."""
    
    @staticmethod
    def events_to_dataframe(
        spark_session: SparkSession,
        events: List[StreamEvent],
        schema: StreamEventSchema
    ) -> DataFrame:
        """Convert list of StreamEvent thành Spark DataFrame.
        
        Args:
            spark_session: SparkSession instance
            events: Danh sách StreamEvent
            schema: StreamEventSchema để định nghĩa schema
            
        Returns:
            DataFrame: Spark DataFrame
        """
        if not events:
            # Trả về empty DataFrame với schema đúng
            spark_schema = SparkSchemaConverter.to_spark_struct_type(schema)
            return spark_session.createDataFrame([], spark_schema)
        
        # Convert events thành rows
        rows = []
        for event in events:
            row = DataFrameConverter._event_to_row(event, schema)
            rows.append(row)
        
        # Tạo DataFrame với schema
        spark_schema = SparkSchemaConverter.to_spark_struct_type(schema)
        return spark_session.createDataFrame(rows, spark_schema)
    
    @staticmethod
    def _event_to_row(event: StreamEvent, schema: StreamEventSchema) -> tuple:
        """Convert một StreamEvent thành row tuple theo schema.
        
        Args:
            event: StreamEvent
            schema: StreamEventSchema
            
        Returns:
            tuple: Row data
        """
        row_data = []
        
        for field_def in schema.fields:
            field_name = field_def.name
            field_value = None
            
            # Lấy giá trị từ event
            if hasattr(event, field_name):
                field_value = getattr(event, field_name)
            elif field_name == "processed_at":
                field_value = datetime.now()
            
            # Convert giá trị theo field type
            if field_value is None:
                row_data.append(None)
            elif field_def.field_type in [FieldType.JSON, FieldType.JSONB]:
                # Convert dict/list thành JSON string
                if isinstance(field_value, (dict, list)):
                    row_data.append(json.dumps(field_value))
                else:
                    row_data.append(str(field_value))
            elif field_def.field_type in [FieldType.TIMESTAMP, FieldType.DATETIME]:
                # Parse timestamp
                if isinstance(field_value, str):
                    row_data.append(datetime.fromisoformat(field_value))
                else:
                    row_data.append(field_value)
            else:
                row_data.append(field_value)
        
        return tuple(row_data)

