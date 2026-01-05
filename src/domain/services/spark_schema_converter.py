"""Service để convert StreamEventSchema thành Spark StructType."""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
)

from ..schemas.stream_event_schema import StreamEventSchema, FieldType


class SparkSchemaConverter:
    """Service để convert domain schema thành Spark schema."""
    
    # Mapping từ FieldType sang Spark types
    SPARK_TYPE_MAP = {
        FieldType.STRING: StringType(),
        FieldType.INTEGER: IntegerType(),
        FieldType.FLOAT: DoubleType(),
        FieldType.BOOLEAN: BooleanType(),
        FieldType.TIMESTAMP: TimestampType(),
        FieldType.DATETIME: TimestampType(),
        FieldType.JSON: StringType(),
        FieldType.JSONB: StringType(),
    }
    
    @classmethod
    def to_spark_struct_type(cls, schema: StreamEventSchema) -> StructType:
        """Convert StreamEventSchema thành Spark StructType.
        
        Args:
            schema: StreamEventSchema
            
        Returns:
            StructType: Spark StructType
        """
        schema_fields = []
        
        for field_def in schema.fields:
            spark_type = cls.SPARK_TYPE_MAP.get(
                field_def.field_type,
                StringType()  # Default fallback
            )
            
            schema_fields.append(
                StructField(field_def.name, spark_type, field_def.nullable)
            )
        
        return StructType(schema_fields)

