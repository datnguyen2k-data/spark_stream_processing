"""Service để quản lý schema evolution."""
from typing import Optional, Dict, Any
import json

from ..schemas.stream_event_schema import StreamEventSchema, FieldDefinition, FieldType


class SchemaEvolutionService:
    """Service để quản lý schema evolution cho sink connectors."""
    
    def __init__(self, current_schema: StreamEventSchema):
        """Khởi tạo service với schema hiện tại.
        
        Args:
            current_schema: Schema hiện tại
        """
        self.current_schema = current_schema
    
    def check_and_evolve(
        self,
        target_schema: StreamEventSchema,
        db_type: str,
        table_name: str,
        connection: Any
    ) -> bool:
        """Kiểm tra và thực hiện schema evolution nếu cần.
        
        Args:
            target_schema: Schema mục tiêu
            db_type: Loại database ('clickhouse', 'postgres', 'iceberg')
            table_name: Tên table
            connection: Database connection object
            
        Returns:
            bool: True nếu có thay đổi schema, False nếu không
        """
        if self.current_schema.version == target_schema.version:
            return False
        
        migrations = self.current_schema.evolve(target_schema)
        
        if not migrations:
            return False
        
        # Thực hiện migrations
        for migration in migrations:
            if migration.startswith("--"):
                # Comment, chỉ log
                print(migration)
                continue
            
            try:
                self._execute_migration(migration, db_type, table_name, connection)
                print(f"Đã thực hiện migration: {migration}")
            except Exception as e:
                print(f"Lỗi khi thực hiện migration {migration}: {e}")
                raise
        
        # Cập nhật schema hiện tại
        self.current_schema = target_schema
        return True
    
    def _execute_migration(
        self,
        migration_sql: str,
        db_type: str,
        table_name: str,
        connection: Any
    ) -> None:
        """Thực hiện migration SQL.
        
        Args:
            migration_sql: SQL statement để migrate
            db_type: Loại database
            table_name: Tên table
            connection: Database connection object (SparkSession)
        """
        full_sql = f"ALTER TABLE {table_name} {migration_sql}"
        
        # Tất cả đều sử dụng Spark SQL
        if db_type in ['clickhouse', 'postgres', 'iceberg']:
            connection.sql(full_sql)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def ensure_table_exists(
        self,
        schema: StreamEventSchema,
        db_type: str,
        table_name: str,
        connection: Any
    ) -> None:
        """Đảm bảo table tồn tại với schema đúng.
        
        Args:
            schema: Schema để tạo table
            db_type: Loại database
            table_name: Tên table
            connection: Database connection object (SparkSession)
        """
        create_sql = schema.to_create_table_sql(table_name, db_type)
        
        try:
            # Tất cả đều sử dụng Spark SQL
            if db_type in ['clickhouse', 'postgres', 'iceberg']:
                statements = [s.strip() for s in create_sql.split(';') if s.strip()]
                for stmt in statements:
                    if stmt:
                        connection.sql(stmt)
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            print(f"Table {table_name} đã được tạo hoặc đã tồn tại với schema version {schema.version}")
        except Exception as e:
            print(f"Lỗi khi tạo table: {e}")
            raise

