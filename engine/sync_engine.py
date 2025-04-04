import time

from db.data_migrator import migrate_data
from db.schema_reader import read_schema
from db.schema_writer import write_schema
from utils.logger import setup_logger

logger = setup_logger()

def sync_databases(source_db_url, target_db_url, source_schema: str, target_schema: str, program_name=""):
    """
    Syncs the source database to the target database.
    :param source_db_url: SQLAlchemy connection URL for the source database.
    :param target_db_url: SQLAlchemy connection URL for the target database.
    :param source_schema: schema name in the source database.
    :param target_schema: schema name in the target database.
    :param program_name: this is just added for logging.
    """
    start_time = time.time()

    logger.info(f"Starting database sync process for {program_name}...")

    # Phase 1: Schema Duplication
    phase1_start = time.time()
    logger.info("Reading schema from source database...")
    schema, tables = read_schema(source_db_url, source_schema)
    logger.info("Writing schema to target database...")
    write_schema(target_db_url, tables, target_schema)
    phase1_end = time.time()
    logger.info(f"Phase 1 completed in {phase1_end - phase1_start:.2f} seconds")

    # Phase 2: Data Duplication

    phase2_start = time.time()
    migrate_data(source_db_url, target_db_url, schema, source_schema, target_schema)
    phase2_end = time.time()
    logger.info(f"Phase 2 completed in {phase2_end - phase2_start:.2f} seconds")

    # Additional phases can be added here in the future

    end_time = time.time()
    logger.info(f"Database sync process completed in {end_time - start_time:.2f} seconds for {program_name}")