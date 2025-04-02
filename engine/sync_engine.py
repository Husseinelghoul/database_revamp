import time
from db.schema_reader import read_schema
from db.schema_writer import write_schema
from db.data_migrator import migrate_data
from utils.logger import setup_logger

logger = setup_logger()

# phases_to_skip = ['phase1','phase2']
phases_to_skip = []

def sync_databases(source_db_url, target_db_url, config):
    """
    Syncs the source database to the target database.
    :param source_db_url: SQLAlchemy connection URL for the source database.
    :param target_db_url: SQLAlchemy connection URL for the target database.
    :param config: Configuration dictionary containing database details and phases to run.
    """
    start_time = time.time()

    logger.info("Starting database sync process...")

    # Phase 1: Schema Duplication
    if 'phase1' not in phases_to_skip:
        phase1_start = time.time()
        logger.info("Reading schema from source database...")
        schema, tables = read_schema(source_db_url, config['source_db']['schema'])
        write_schema(target_db_url, tables, config['target_db']['schema'])
        phase1_end = time.time()
        logger.info(f"Phase 1 completed in {phase1_end - phase1_start:.2f} seconds")

    # Phase 2: Data Duplication
    if 'phase2' not in phases_to_skip:
        phase2_start = time.time()
        if 'phase1' not in phases_to_skip:
            schema, _ = read_schema(source_db_url, config['source_db']['schema'])
        migrate_data(source_db_url, target_db_url, schema, config)
        phase2_end = time.time()
        logger.info(f"Phase 2 completed in {phase2_end - phase2_start:.2f} seconds")

    # Additional phases can be added here in the future

    end_time = time.time()
    logger.info(f"Database sync process completed in {end_time - start_time:.2f} seconds")