"""Schema definition cho StreamEvent - Schema as Code."""
from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict
from enum import Enum


class FieldType(Enum):
    """Các loại field type."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    JSON = "json"
    JSONB = "jsonb"
    DATETIME = "datetime"


@dataclass
class FieldDefinition:
    """Định nghĩa một field trong schema."""
    name: str
    field_type: FieldType
    nullable: bool = True
    default_value: Optional[Any] = None
    description: Optional[str] = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    
    def to_sql_type(self, db_type: str) -> str:
        """Chuyển đổi field type sang SQL type dựa trên database.
        
        Args:
            db_type: Loại database ('clickhouse', 'postgres', 'iceberg')
            
        Returns:
            str: SQL type string
        """
        type_mapping = {
            'clickhouse': {
                FieldType.STRING: 'String',
                FieldType.INTEGER: 'Int64',
                FieldType.FLOAT: 'Float64',
                FieldType.BOOLEAN: 'UInt8',
                FieldType.TIMESTAMP: 'DateTime',
                FieldType.DATETIME: 'DateTime',
                FieldType.JSON: 'String',
            },
            'postgres': {
                FieldType.STRING: 'VARCHAR(255)',
                FieldType.INTEGER: 'INTEGER',
                FieldType.FLOAT: 'DOUBLE PRECISION',
                FieldType.BOOLEAN: 'BOOLEAN',
                FieldType.TIMESTAMP: 'TIMESTAMP',
                FieldType.DATETIME: 'TIMESTAMP',
                FieldType.JSON: 'JSONB',
                FieldType.JSONB: 'JSONB',
            },
            'iceberg': {
                FieldType.STRING: 'STRING',
                FieldType.INTEGER: 'BIGINT',
                FieldType.FLOAT: 'DOUBLE',
                FieldType.BOOLEAN: 'BOOLEAN',
                FieldType.TIMESTAMP: 'TIMESTAMP',
                FieldType.DATETIME: 'TIMESTAMP',
                FieldType.JSON: 'STRING',
            }
        }
        
        sql_type = type_mapping.get(db_type, {}).get(self.field_type, 'STRING')
        
        # Thêm nullable constraint
        if not self.nullable and db_type == 'postgres':
            sql_type += ' NOT NULL'
        
        return sql_type


@dataclass
class StreamEventSchema:
    """Schema definition cho StreamEvent - Schema as Code."""
    version: int = 1
    fields: List[FieldDefinition] = field(default_factory=list)
    
    def __post_init__(self):
        """Khởi tạo schema mặc định nếu chưa có fields."""
        if not self.fields:
            self.fields = self._get_default_fields()
    
    @classmethod
    def _get_default_fields(cls) -> List[FieldDefinition]:
        """Lấy danh sách fields mặc định."""
        return [
            FieldDefinition(
                name="event_id",
                field_type=FieldType.STRING,
                nullable=False,
                description="Unique identifier cho event"
            ),
            FieldDefinition(
                name="event_type",
                field_type=FieldType.STRING,
                nullable=False,
                description="Loại event"
            ),
            FieldDefinition(
                name="timestamp",
                field_type=FieldType.TIMESTAMP,
                nullable=False,
                description="Thời gian event xảy ra"
            ),
            FieldDefinition(
                name="payload",
                field_type=FieldType.JSON,
                nullable=True,
                description="Payload data của event"
            ),
            FieldDefinition(
                name="source",
                field_type=FieldType.STRING,
                nullable=True,
                description="Nguồn của event"
            ),
            FieldDefinition(
                name="metadata",
                field_type=FieldType.JSON,
                nullable=True,
                description="Metadata của event"
            ),
            FieldDefinition(
                name="processed_at",
                field_type=FieldType.TIMESTAMP,
                nullable=True,
                default_value="CURRENT_TIMESTAMP",
                description="Thời gian xử lý event"
            )
        ]
    
    def get_field(self, name: str) -> Optional[FieldDefinition]:
        """Lấy field definition theo tên."""
        for field_def in self.fields:
            if field_def.name == name:
                return field_def
        return None
    
    def add_field(self, field_def: FieldDefinition) -> None:
        """Thêm field mới vào schema."""
        if self.get_field(field_def.name):
            raise ValueError(f"Field {field_def.name} đã tồn tại")
        self.fields.append(field_def)
    
    def remove_field(self, name: str) -> None:
        """Xóa field khỏi schema."""
        self.fields = [f for f in self.fields if f.name != name]
    
    def evolve(self, new_schema: 'StreamEventSchema') -> List[str]:
        """So sánh schema và trả về danh sách migration SQL.
        
        Args:
            new_schema: Schema mới
            
        Returns:
            List[str]: Danh sách SQL statements để migrate
        """
        migrations = []
        
        # Tìm fields mới
        existing_field_names = {f.name for f in self.fields}
        new_field_names = {f.name for f in new_schema.fields}
        
        added_fields = new_field_names - existing_field_names
        removed_fields = existing_field_names - new_field_names
        
        # Tạo migration cho fields mới
        for field_name in added_fields:
            field_def = new_schema.get_field(field_name)
            if field_def:
                migrations.append(f"ADD COLUMN {field_name} {field_def.to_sql_type('postgres')}")
        
        # Note: Thông thường không xóa columns trong production
        # Nhưng có thể log warning
        if removed_fields:
            migrations.append(f"-- WARNING: Removed fields: {', '.join(removed_fields)}")
        
        return migrations
    
    def to_create_table_sql(self, table_name: str, db_type: str) -> str:
        """Tạo SQL CREATE TABLE statement.
        
        Args:
            table_name: Tên table
            db_type: Loại database ('clickhouse', 'postgres', 'iceberg')
            
        Returns:
            str: SQL CREATE TABLE statement
        """
        if db_type == 'clickhouse':
            return self._to_clickhouse_sql(table_name)
        elif db_type == 'postgres':
            return self._to_postgres_sql(table_name)
        elif db_type == 'iceberg':
            return self._to_iceberg_sql(table_name)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _to_clickhouse_sql(self, table_name: str) -> str:
        """Tạo ClickHouse CREATE TABLE SQL."""
        columns = []
        for field in self.fields:
            sql_type = field.to_sql_type('clickhouse')
            default = f" DEFAULT {field.default_value}" if field.default_value else ""
            columns.append(f"    {field.name} {sql_type}{default}")
        
        columns_str = ",\n".join(columns)
        return f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_str}
) ENGINE = MergeTree()
ORDER BY (timestamp, event_id)
PARTITION BY toYYYYMM(timestamp)"""
    
    def _to_postgres_sql(self, table_name: str) -> str:
        """Tạo PostgreSQL CREATE TABLE SQL."""
        columns = []
        primary_key = None
        
        for field in self.fields:
            sql_type = field.to_sql_type('postgres')
            default = f" DEFAULT {field.default_value}" if field.default_value else ""
            
            # Tìm primary key
            if field.name == "event_id" and not field.nullable:
                primary_key = field.name
            
            columns.append(f"    {field.name} {sql_type}{default}")
        
        columns_str = ",\n".join(columns)
        
        if primary_key:
            columns_str += f",\n    PRIMARY KEY ({primary_key})"
        
        # Thêm indexes
        indexes = []
        if self.get_field("timestamp"):
            indexes.append(f"CREATE INDEX IF NOT EXISTS idx_timestamp ON {table_name}(timestamp);")
        if self.get_field("event_type"):
            indexes.append(f"CREATE INDEX IF NOT EXISTS idx_event_type ON {table_name}(event_type);")
        
        indexes_str = "\n".join(indexes)
        
        return f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_str}
);

{indexes_str}"""
    
    def _to_iceberg_sql(self, table_name: str) -> str:
        """Tạo Iceberg CREATE TABLE SQL."""
        columns = []
        for field in self.fields:
            sql_type = field.to_sql_type('iceberg')
            nullable = "" if field.nullable else " NOT NULL"
            columns.append(f"    {field.name} {sql_type}{nullable}")
        
        columns_str = ",\n".join(columns)
        return f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_str}
) USING ICEBERG
PARTITIONED BY (days(timestamp))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)"""

