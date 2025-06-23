import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import QueuePool

from config.constants import BATCH_SIZE, CHUNK_SIZE
from utils.logger import setup_logger

logger = setup_logger()


class MigrationError(Exception):
    pass


def get_identity_columns(inspector, table_name, schema):
    return [
        col["name"]
        for col in inspector.get_columns(table_name, schema=schema)
        if col.get("autoincrement")
    ]


def get_column_count(inspector, table_name, schema):
    columns = inspector.get_columns(table_name, schema=schema)
    return len(columns)


def has_project_name_column(inspector, table_name, schema):
    """Check if the table has a project_name column"""
    columns = inspector.get_columns(table_name, schema=schema)
    return any(col["name"].lower() == "project_name" for col in columns)


def should_get_all_projects(project_names):
    """Check if we should get all projects (wildcard or empty list)"""
    return not project_names or (len(project_names) == 1 and project_names[0] == "*")


def build_project_filter_query(table_name, schema, project_names):
    """Build SQL query with project_name filter"""
    if should_get_all_projects(project_names):
        return f"SELECT * FROM {schema}.{table_name}"
    
    # Create parameterized query for security
    placeholders = ", ".join([f"'{name}'" for name in project_names])
    return f"SELECT * FROM {schema}.{table_name} WHERE project_name IN ({placeholders})"


def migrate_table(
    table_name,
    source_engine,
    target_engine,
    source_schema,
    target_schema,
    project_names=None,
    chunksize=CHUNK_SIZE,
    batch_size=BATCH_SIZE,
):
    try:
        logger.debug(f"Duplicating data for table: {table_name}")

        inspector = inspect(source_engine)
        identity_columns = get_identity_columns(inspector, table_name, source_schema)

        if get_column_count(inspector, table_name, source_schema) > 25:
            chunksize = int(chunksize / 10)
            batch_size = int(batch_size / 10)

        # Check if table has project_name column and build appropriate query
        has_project_column = has_project_name_column(inspector, table_name, source_schema)
        
        if has_project_column and project_names:
            # Use custom SQL query with project filter
            query = build_project_filter_query(table_name, source_schema, project_names)
            logger.debug(f"Filtering table {table_name} by projects: {project_names}")
            
            # Read data with custom query
            data_reader = pd.read_sql(
                query, 
                source_engine, 
                chunksize=chunksize
            )
        else:
            # Use standard table read (no project filter needed or available)
            if has_project_column:
                logger.debug(f"Table {table_name} has project_name column but no filter specified - getting all data")
            else:
                logger.debug(f"Table {table_name} has no project_name column - getting all data")
                
            data_reader = pd.read_sql_table(
                table_name, 
                source_engine, 
                schema=source_schema, 
                chunksize=chunksize
            )

        # Process chunks
        for chunk in data_reader:
            with target_engine.begin() as conn:
                if identity_columns:
                    conn.execute(
                        text(f"SET IDENTITY_INSERT {target_schema}.{table_name} ON")
                    )

                for i in range(0, len(chunk), batch_size):
                    batch = chunk.iloc[i : i + batch_size]
                    batch.to_sql(
                        table_name,
                        conn,
                        schema=target_schema,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )

                if identity_columns:
                    conn.execute(
                        text(f"SET IDENTITY_INSERT {target_schema}.{table_name} OFF")
                    )

        logger.debug(f"Data duplication completed for table: {table_name}")

    except Exception as e:
        logger.error(f"Failed to duplicate data for table: {table_name}. Error: {e}")
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
    Migrates data from the source database to the target database.
    Handles identity constraints and migrates tables in parallel.
    Skips any table found in the config/schema_changes/table_drops.csv file.
    
    Args:
        source_db_url: Source database connection URL
        target_db_url: Target database connection URL
        schema: Dictionary of table schemas
        source_schema: Source schema name
        target_schema: Target schema name
        project_names: List of project names to filter by. If None, ['*'], or empty, gets all data.
                      If contains '*' as single element, gets all data.
                      Otherwise filters tables with project_name column by specified projects.
    """

    # 🔧 Create engines with tuned connection pools
    source_engine = create_engine(
        source_db_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        poolclass=QueuePool,
    )

    target_engine = create_engine(
        target_db_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        poolclass=QueuePool,
    )

    # Handle project names parameter
    if project_names is None:
        project_names = []
    
    logger.debug(f"Migration starting with project filter: {project_names if project_names else 'ALL PROJECTS'}")

    # 📄 Load dropped tables list
    try:
        dropped_df = pd.read_csv("config/schema_changes/table_drops.csv")
        dropped_tables = set(dropped_df["table_name"].dropna().str.strip())
    except Exception as e:
        logger.error(f"Could not read dropped tables list: {e}")
        raise e

    # 🧹 Remove dropped tables and known exclusions
    for table in list(schema.keys()):
        if table in dropped_tables:
            logger.debug(
                f"Skipping data copy for table '{table}' as it is marked for drop."
            )
            del schema[table]

    if "Sessions" in schema:
        del schema["Sessions"]

    # 🚀 Migrate tables in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(
                migrate_table,
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
                for f in futures:
                    f.cancel()
                source_engine.dispose()
                target_engine.dispose()
                sys.exit(1)

    # ✅ Clean up connections
    source_engine.dispose()
    target_engine.dispose()
    
    logger.info("Migration completed successfully")