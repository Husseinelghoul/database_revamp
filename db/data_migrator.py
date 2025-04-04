import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from config.constants import BATCH_SIZE, CHUNK_SIZE
from utils.logger import setup_logger


logger = setup_logger()

class MigrationError(Exception):
    pass

def get_identity_columns(inspector, table_name, schema):
    return [col['name'] for col in inspector.get_columns(table_name, schema=schema) if col.get('autoincrement')]

def get_column_count(inspector, table_name, schema):
    columns = inspector.get_columns(table_name, schema=schema)
    return len(columns)

def migrate_table(table_name, source_engine, target_engine, source_schema, target_schema, chunksize=CHUNK_SIZE, batch_size=BATCH_SIZE):
    try:
        logger.info(f"Duplicating data for table: {table_name}")
        
        inspector = inspect(source_engine)
        identity_columns = get_identity_columns(inspector, table_name, source_schema)
        if get_column_count(inspector, table_name, source_schema) > 25:
            chunksize = int(chunksize/10)
            batch_size = int(batch_size/10)
        for chunk in pd.read_sql_table(table_name, source_engine, schema=source_schema, chunksize=chunksize):
            with target_engine.begin() as conn:
                if identity_columns:
                    conn.execute(text(f"SET IDENTITY_INSERT {target_schema}.{table_name} ON"))
                
                # Insert data in smaller batches
                for i in range(0, len(chunk), batch_size):
                    batch = chunk.iloc[i:i+batch_size]
                    batch.to_sql(table_name, conn, schema=target_schema, if_exists='append', index=False, method='multi')
                
                if identity_columns:
                    conn.execute(text(f"SET IDENTITY_INSERT {target_schema}.{table_name} OFF"))
        
        logger.info(f"Data duplication completed for table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to duplicate data for table: {table_name}. Error: {e}")
        raise MigrationError(f"Failed to migrate table {table_name}: {str(e)}")

def migrate_data(source_db_url, target_db_url, schema, source_schema: str, target_schema: str):
    """
    Migrates data from the source database to the target database.
    Handles identity constraints and migrates tables in parallel.
    """
    source_engine = create_engine(source_db_url)
    target_engine = create_engine(target_db_url)

    logger.info("Starting Phase 2: Data Duplication...")

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                migrate_table, 
                table_name, 
                source_engine, 
                target_engine, 
                source_schema, 
                target_schema
            ): table_name for table_name in schema.keys()
        }
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
            except MigrationError as e:
                logger.error(f"Migration failed: {str(e)}")
                for f in futures:
                    f.cancel()
                sys.exit(1)

    logger.info("Phase 2: Data Duplication completed.")