from sqlalchemy import create_engine

from utils.logger import setup_logger

logger = setup_logger()



def write_schema(target_db_url, tables, target_schema):
    """
    Writes the schema to the target MSSQL database using dynamically generated SQLAlchemy table objects.
    Uses the schema specified in the configuration.
    Aborts on the first failure by raising an exception.

    :param target_db_url: SQLAlchemy connection URL for the target database.
    :param tables: Dictionary containing dynamically generated SQLAlchemy table objects.
    :param target_schema: Target schema name
    :param logger: Logger instance for logging progress.
    :raises: Exception if any table creation fails.
    """
    engine = create_engine(target_db_url)
    
    logger.info(f"Starting Phase 1: Schema Duplication to schema '{target_schema}'...")
    
    for table_name, table in tables.items():
        logger.debug(f"Duplicating schema for table: {table_name}")
        try:
            # Change the schema of the table
            table.schema = target_schema
            table.create(bind=engine, checkfirst=True)
            logger.debug(f"Schema duplication completed for table: {table_name} in schema {target_schema}")
        except Exception as e:
            logger.error(f"Failed to duplicate schema for table: {table_name} in schema {target_schema}. Error: {e}")
            raise  # This will abort the process on the first failure
    
    logger.info(f"Phase 1: Schema Duplication to schema '{target_schema}' completed.")