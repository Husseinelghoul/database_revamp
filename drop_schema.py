TARGET = 'pulse'

import sys
import time
from sqlalchemy import create_engine, text

# Import from existing modules
from config.db_config import build_connection_url, load_config
from utils.logger import setup_logger

logger = setup_logger()

def drop_all_schema_objects(db_url: str, schema: str):
    """
    Fast and efficient method to drop all objects in a schema.
    Handles all types of objects in the correct order.
    """
    engine = create_engine(db_url)
    start_time = time.time()
    
    try:
        with engine.begin() as conn:
            # 1. Disable all foreign key constraints
            logger.info(f"Disabling all foreign key constraints in schema {schema}...")
            disable_fk_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
                                NOCHECK CONSTRAINT [' + name + '];'
                FROM sys.foreign_keys
                WHERE OBJECT_SCHEMA_NAME(parent_object_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(disable_fk_sql, {"schema": schema})
            
            # 2. Generate DROP statements for all foreign keys
            logger.info(f"Dropping all foreign keys in schema {schema}...")
            drop_fk_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
                                DROP CONSTRAINT [' + name + '];'
                FROM sys.foreign_keys
                WHERE OBJECT_SCHEMA_NAME(parent_object_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_fk_sql, {"schema": schema})
            
            # 3. Drop all check constraints
            logger.info(f"Dropping all check constraints in schema {schema}...")
            drop_check_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
                                DROP CONSTRAINT [' + name + '];'
                FROM sys.check_constraints
                WHERE OBJECT_SCHEMA_NAME(parent_object_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_check_sql, {"schema": schema})
            
            # 4. Drop all default constraints
            logger.info(f"Dropping all default constraints in schema {schema}...")
            drop_default_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
                                DROP CONSTRAINT [' + name + '];'
                FROM sys.default_constraints
                WHERE OBJECT_SCHEMA_NAME(parent_object_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_default_sql, {"schema": schema})
            
            # 5. Drop all primary key constraints
            logger.info(f"Dropping all primary key constraints in schema {schema}...")
            drop_pk_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
                                DROP CONSTRAINT [' + name + '];'
                FROM sys.key_constraints
                WHERE OBJECT_SCHEMA_NAME(parent_object_id) = :schema
                AND type = 'PK';
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_pk_sql, {"schema": schema})
            
            # 6. Drop all unique constraints
            logger.info(f"Dropping all unique constraints in schema {schema}...")
            drop_unique_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] 
                                DROP CONSTRAINT [' + name + '];'
                FROM sys.key_constraints
                WHERE OBJECT_SCHEMA_NAME(parent_object_id) = :schema
                AND type = 'UQ';
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_unique_sql, {"schema": schema})
            
            # 7. Drop all tables
            logger.info(f"Dropping all tables in schema {schema}...")
            drop_tables_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'DROP TABLE [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
                FROM sys.tables
                WHERE SCHEMA_NAME(schema_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_tables_sql, {"schema": schema})
            
            # 8. Drop all views
            logger.info(f"Dropping all views in schema {schema}...")
            drop_views_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'DROP VIEW [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
                FROM sys.views
                WHERE SCHEMA_NAME(schema_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_views_sql, {"schema": schema})
            
            # 9. Drop all stored procedures
            logger.info(f"Dropping all stored procedures in schema {schema}...")
            drop_procs_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'DROP PROCEDURE [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
                FROM sys.procedures
                WHERE SCHEMA_NAME(schema_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_procs_sql, {"schema": schema})
            
            # 10. Drop all functions
            logger.info(f"Dropping all functions in schema {schema}...")
            drop_funcs_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'DROP FUNCTION [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
                FROM sys.objects
                WHERE type_desc LIKE '%FUNCTION%'
                AND SCHEMA_NAME(schema_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_funcs_sql, {"schema": schema})
            
            # 11. Drop all user-defined types
            logger.info(f"Dropping all user-defined types in schema {schema}...")
            drop_types_sql = text(f"""
                DECLARE @sql NVARCHAR(MAX) = N'';
                
                SELECT @sql += N'DROP TYPE [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
                FROM sys.types
                WHERE is_user_defined = 1
                AND SCHEMA_NAME(schema_id) = :schema;
                
                EXEC sp_executesql @sql;
            """)
            conn.execute(drop_types_sql, {"schema": schema})
            
            # 12. Check if anything remains and report
            logger.info(f"Checking for any remaining objects in schema {schema}...")
            check_sql = text(f"""
                SELECT 
                    type_desc, 
                    COUNT(*) as ObjectCount
                FROM sys.objects
                WHERE SCHEMA_NAME(schema_id) = :schema
                GROUP BY type_desc;
            """)
            remaining = conn.execute(check_sql, {"schema": schema}).fetchall()
            
            if remaining:
                logger.warning(f"Some objects still remain in schema {schema}:")
                for obj_type, count in remaining:
                    logger.warning(f"  - {count} {obj_type}")
            else:
                logger.info(f"Schema {schema} has been completely purged of all objects")
            
        end_time = time.time()
        logger.info(f"Schema cleanup completed in {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Failed to drop schema objects: {e}")
        logger.error(f"Error details: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Load the configuration from config.json
    logger.info("Starting schema cleanup...")
    config = load_config("config.json")
    
    # Get database configuration
    db_config = config.get(f"{TARGET}_target_db", {})
    if not db_config:
        logger.error("No target database configuration found in config")
        sys.exit(1)
    
    # Build connection URL
    db_url = build_connection_url(db_config)
    
    # Drop schema objects
    drop_all_schema_objects(db_url, db_config['schema'])
    
    logger.info("Schema cleanup completed")