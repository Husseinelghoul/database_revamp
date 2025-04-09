import time

from db.data_migrator import migrate_data
from db.drop_operations import drop_columns, drop_tables
from db.schema_reader import read_schema
from db.schema_writer import write_schema
from utils.logger import setup_logger

logger = setup_logger()

def sync_databases(source_db_url, target_db_url, source_schema: str, target_schema: str, application: str, phases_to_skip: list):
    """
    Syncs the source database to the target database.
    :param source_db_url: SQLAlchemy connection URL for the source database.
    :param target_db_url: SQLAlchemy connection URL for the target database.
    :param source_schema: schema name in the source database.
    :param target_schema: schema name in the target database.
    :param application: this is just added for logging and is either insigths or pulse.
    :param phases_to_skip: endicates whicb phases not to run, configured in constanrts
    """
    start_time = time.time()

    logger.info(f"Starting database sync process for {application}...")

    # Phase 1: Schema Duplication
    if "phase1" in phases_to_skip:
        logger.info("Skipping Phase 1: Schema Duplication")
    else:
        phase1_start = time.time()
        logger.info("Phase 1: Reading schema from source database...")
        schema, tables = read_schema(source_db_url, source_schema)
        logger.info("Writing schema to target database...")
        write_schema(target_db_url, tables, target_schema)
        phase1_end = time.time()
        logger.info(f"Phase 1 completed in {phase1_end - phase1_start:.2f} seconds")

    # Phase 2: Data Duplication
    if "phase2" in phases_to_skip:
        logger.info("Skipping Phase 2: Data Duplication")
    else:
        logger.info("Phase 2: Migrating data to target database...")
        phase2_start = time.time()
        migrate_data(source_db_url, target_db_url, schema, source_schema, target_schema)
        phase2_end = time.time()
        logger.info(f"Phase 2 completed in {phase2_end - phase2_start:.2f} seconds")

    # Phase 3: Table and Column Removal
    if "phase3" in phases_to_skip:
        logger.info("Skipping Phase 3: Table and Column Removal")
    else:
        phase3_start = time.time()
        logger.info(f"Phase 2: Dropping tables and columns")
        logger.info(f"Dropping tables")
        drop_tables(target_db_url, target_schema, application)
        logger.info(f"Dropping schemas")
        drop_columns(target_db_url, target_schema, application)
        phase3_end = time.time()
        logger.info(f"Phase 3 completed in {phase3_end - phase3_start:.2f} seconds")

    end_time = time.time()
    logger.info(f"Database sync process completed in {end_time - start_time:.2f} seconds for {application}")