import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SAWarning
from sqlalchemy.pool import QueuePool

from config.constants import MIGRATION_CHUNK_SIZE
from utils.logger import setup_logger

logger = setup_logger()

class MigrationError(Exception):
    pass

def migrate_table_optimized(
    table_name,
    source_engine,
    target_engine,
    source_schema,
    target_schema,
    project_names=None,
    chunksize=MIGRATION_CHUNK_SIZE,
):
    """
    Final version with the UserWarning correctly suppressed.
    """
    try:
        logger.debug(f"Starting migration for table: {table_name}")

        # === Step 1: Inspect tables to understand their structure ===
        target_inspector = inspect(target_engine)
        try:
            target_cols_info = target_inspector.get_columns(table_name, schema=target_schema)
            target_column_names = {col["name"] for col in target_cols_info}
        except Exception as e:
            raise MigrationError(f"Could not inspect target table {table_name}: {e}")

        # === Step 2: Build and execute the source query ===
        source_inspector = inspect(source_engine)
        source_columns = source_inspector.get_columns(table_name, schema=source_schema)
        has_project_column = any(col["name"].lower() == "project_name" for col in source_columns)

        if has_project_column and project_names and project_names != ['*']:
            query = text(f'SELECT * FROM "{source_schema}"."{table_name}" WHERE project_name IN :project_names')
            params = {"project_names": tuple(project_names)}
        else:
            query = text(f'SELECT * FROM "{source_schema}"."{table_name}"')
            params = {}
        
        data_reader = pd.read_sql(query, source_engine, params=params, chunksize=chunksize)

        # === Step 3: Perform the migration in a SINGLE transaction ===
        with target_engine.begin() as conn:
            try:
                first_chunk = next(data_reader)
            except StopIteration:
                logger.debug(f"Table {table_name} is empty. Nothing to migrate.")
                return

            target_identity_col_list = [c["name"] for c in target_cols_info if c.get("autoincrement")]
            target_identity_col = target_identity_col_list[0] if target_identity_col_list else None
            should_set_identity_insert = target_identity_col and target_identity_col in first_chunk.columns

            if should_set_identity_insert:
                logger.debug(f"Setting IDENTITY_INSERT ON for {table_name}")
                conn.execute(text(f'SET IDENTITY_INSERT "{target_schema}"."{table_name}" ON'))

            def process_chunk(chunk):
                mappable_columns = [col for col in chunk.columns if col in target_column_names]
                if not mappable_columns:
                    return
                
                # === THE FIX: Correctly target UserWarning ===
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        r"The provided table name .* is not found exactly as such",
                        UserWarning, # This was changed from SAWarning
                    )
                    chunk[mappable_columns].to_sql(
                        table_name, conn, schema=target_schema, if_exists="append", index=False
                    )

            process_chunk(first_chunk)
            for chunk in data_reader:
                process_chunk(chunk)

            if should_set_identity_insert:
                logger.debug(f"Setting IDENTITY_INSERT OFF for {table_name}")
                conn.execute(text(f'SET IDENTITY_INSERT "{target_schema}"."{table_name}" OFF'))

        logger.debug(f"Successfully migrated data for table: {table_name}")

    except Exception as e:
        logger.error(f"Failed to migrate data for table: {table_name}. Error: {e}")
        raise MigrationError(f"Failed to migrate table {table_name}: {str(e)}")
    
def migrate_data(
    source_db_url, 
    target_db_url, 
    schema, 
    source_schema: str, 
    target_schema: str,
    project_names: list = None
):
    """
    Coordinates the migration process by calling the optimized worker function
    for each table using a thread pool.
    """    
    source_engine = create_engine(
        source_db_url,
        connect_args={"charset": "utf8"},
        poolclass=QueuePool,
        fast_executemany=True
    )
    target_engine = create_engine(
        target_db_url,
        connect_args={"charset": "utf8"},
        poolclass=QueuePool,
        fast_executemany=True
    )

    if project_names is None:
        project_names = []
    
    logger.debug(f"Migration starting with project filter: {project_names if project_names else 'ALL PROJECTS'}")

    try:
        dropped_df = pd.read_csv("config/schema_changes/table_drops.csv")
        dropped_tables = set(dropped_df["table_name"].dropna().str.strip())
    except Exception as e:
        logger.error(f"Could not read dropped tables list: {e}")
        raise e

    for table in list(schema.keys()):
        if table in dropped_tables or '2025' in table or 'bkp' in table.lower():
            del schema[table]
    if "Sessions" in schema:
        del schema["Sessions"]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(
                migrate_table_optimized,
                table_name,
                source_engine,
                target_engine,
                source_schema,
                target_schema,
                project_names,
            ): table_name
            for table_name in schema
        }

        for future in as_completed(futures):
            try:
                future.result()
            except MigrationError as e:
                logger.error(f"Migration failed: {str(e)}")
                # Properly shut down other tasks on failure
                for f in futures:
                    if not f.done():
                        f.cancel()
                sys.exit(1)

    source_engine.dispose()
    target_engine.dispose()
    
    logger.debug("Migration completed successfully")