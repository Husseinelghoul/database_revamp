import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import bindparam, create_engine, inspect, text
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
    periods=None,  # <-- New parameter for periods
    chunksize=MIGRATION_CHUNK_SIZE,
):
    """
    Final version with UserWarning suppressed and dynamic IN clauses for both
    project_name and period.
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

        # === Step 2: Dynamically build the source query based on available filters ===
        source_inspector = inspect(source_engine)
        source_columns = source_inspector.get_columns(table_name, schema=source_schema)
        source_column_names_lower = {col["name"].lower() for col in source_columns}

        has_project_column = "project_name" in source_column_names_lower
        has_period_column = "period" in source_column_names_lower

        # --- Dynamic WHERE clause construction ---
        base_query = f'SELECT * FROM "{source_schema}"."{table_name}"'
        where_clauses = []
        params = {}
        expanding_params = []

        # Add project_name filter if applicable
        if has_project_column and project_names and project_names != ['*']:
            where_clauses.append("project_name IN :project_list")
            params["project_list"] = project_names
            expanding_params.append(bindparam('project_list', expanding=True))

        # Add period filter if applicable
        if has_period_column and periods and periods != ['*']:
            where_clauses.append("period IN :period_list")
            params["period_list"] = periods
            expanding_params.append(bindparam('period_list', expanding=True))

        # Combine clauses and finalize the query
        if where_clauses:
            query_string = f"{base_query} WHERE {' AND '.join(where_clauses)}"
            query = text(query_string)
            if expanding_params:
                query = query.bindparams(*expanding_params)
        else:
            query = text(base_query)
        # --- End of dynamic query construction ---
        
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
                
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        r"The provided table name .* is not found exactly as such",
                        UserWarning,
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
    project_names: list = None,
    periods: list = None  # <-- New parameter for periods
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
    if periods is None:  # <-- Initialize periods list
        periods = []
    
    logger.debug(f"Migration starting with project filter: {project_names if project_names else 'ALL PROJECTS'}")
    logger.debug(f"Migration starting with period filter: {periods if periods else 'ALL PERIODS'}")

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
                periods, # <-- Pass the periods list to the worker
            ): table_name
            for table_name in schema
        }

        for future in as_completed(futures):
            try:
                future.result()
            except MigrationError as e:
                logger.error(f"Migration failed: {str(e)}")
                for f in futures:
                    if not f.done():
                        f.cancel()
                sys.exit(1)

    source_engine.dispose()
    target_engine.dispose()
    
    logger.debug("Migration completed successfully")