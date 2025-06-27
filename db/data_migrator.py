import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import QueuePool

from config.constants import MIGRATION_CHUNK_SIZE
from utils.logger import setup_logger

logger = setup_logger()

class MigrationError(Exception):
    pass

# This version is much more efficient as it performs all writes for a table
# within a single transaction, dramatically reducing database overhead.
def migrate_table_optimized(
    table_name,
    source_engine,
    target_engine,
    source_schema,
    target_schema,
    project_names=None,
    chunksize=MIGRATION_CHUNK_SIZE, # A larger, single chunksize is better now
):
    """
    An optimized version of migrate_table that uses a single transaction
    and bulk operations for maximum efficiency.
    """
    try:
        logger.debug(f"Migrating data for table: {table_name}")

        # === Step 1: Efficiently inspect the table ONCE ===
        inspector = inspect(source_engine)
        try:
            columns = inspector.get_columns(table_name, schema=source_schema)
        except Exception as e:
            raise MigrationError(f"Could not inspect source table {table_name}: {e}")

        identity_columns = [col["name"] for col in columns if col.get("autoincrement")]
        has_project_column = any(col["name"].lower() == "project_name" for col in columns)

        # === Step 2: Build the source query ===
        if has_project_column and project_names and project_names != ['*']:
            placeholders = ", ".join([f"'{name}'" for name in project_names])
            query = f'SELECT * FROM "{source_schema}"."{table_name}" WHERE project_name IN ({placeholders})'
            logger.debug(f"Reading from {table_name} with project filter.")
        else:
            query = f'SELECT * FROM "{source_schema}"."{table_name}"'
            logger.debug(f"Reading all data from {table_name}.")
            
        # Create a data reader iterator
        data_reader = pd.read_sql(query, source_engine, chunksize=chunksize)

        # === Step 3: Perform the migration in a SINGLE transaction ===
        with target_engine.begin() as conn: # The transaction starts here
            
            # Set IDENTITY_INSERT ONCE at the beginning if needed
            if identity_columns:
                logger.debug(f"Setting IDENTITY_INSERT ON for {table_name}")
                conn.execute(text(f'SET IDENTITY_INSERT "{target_schema}"."{table_name}" ON'))

            # Process chunks and append within the single transaction
            for chunk in data_reader:
                # The 'to_sql' method is a highly optimized bulk insert.
                # The inner 'batch' loop from the original code is not needed.
                chunk.to_sql(
                    table_name,
                    conn,
                    schema=target_schema,
                    if_exists="append",
                    index=False,
                )
            
            # Set IDENTITY_INSERT OFF ONCE at the end
            if identity_columns:
                logger.debug(f"Setting IDENTITY_INSERT OFF for {table_name}")
                conn.execute(text(f'SET IDENTITY_INSERT "{target_schema}"."{table_name}" OFF'))

        # The transaction is automatically committed here when the 'with' block exits successfully
        logger.debug(f"Data migration completed for table: {table_name}")

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
    (This function remains largely the same, but now calls the optimized worker function)
    """
    source_engine = create_engine(source_db_url, poolclass=QueuePool)
    target_engine = create_engine(target_db_url, poolclass=QueuePool)

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
        if table in dropped_tables:
            del schema[table]
    if "Sessions" in schema:
        del schema["Sessions"]

    # The ThreadPoolExecutor is now effective because the task it's running is efficient.
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(
                migrate_table_optimized, # <-- Calling the new, fast function
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
    
    logger.info("Migration completed successfully")