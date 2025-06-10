import time

from db.data_insertion import populate_master_tables
from db.data_integrity import (add_primary_keys,
                               implement_many_to_many_relations,
                               implement_one_to_many_relations)
from db.data_migrator import migrate_data
from db.data_quality import apply_constraints, change_data_types
from db.drop_operations import drop_columns, drop_tables
from db.rename_operations import rename_columns, rename_tables
from db.schema_changes import split_columns
from db.schema_writer import read_schema, write_schema
from db.sync_master_tables import sync_master_tables
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
        phase_start = time.time()
        logger.info("Phase 1: Reading schema from source database...")
        schema, tables = read_schema(source_db_url, source_schema)
        logger.info("Writing schema to target database...")
        if application == "insights":
            filtered_tables = {key: value for key, value in tables.items() if 'master' not in key.lower()}
            tables = filtered_tables
        write_schema(target_db_url, tables, target_schema)
        if application == "insights":
            logger.info("Writing master tables into insights")
            sync_master_tables()
        phase_end = time.time()
        logger.info(f"Phase 1 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 2: Data Duplication
    if "phase2" in phases_to_skip:
        logger.info("Skipping Phase 2: Data Duplication")
    else:
        logger.info("Phase 2: Migrating data to target database...")
        if application == "blaaa": #TODO: fixme
            logger.info("Skipping Phase 2 for Insights")
        else:
            phase_start = time.time()
            if application == "insights":
                filtered_schema = {key: value for key, value in schema.items() if "master" not in key.lower()}
                schema = filtered_schema
            migrate_data(source_db_url, target_db_url, schema, source_schema, target_schema)
            phase_end = time.time()
            logger.info(f"Phase 2 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 3: Table and Column Removal
    if "phase3" in phases_to_skip:
        logger.info("Skipping Phase 3: Table and Column Removal")
    else:
        phase_start = time.time()
        logger.info(f"Phase 3: Dropping tables and columns")
        logger.info(f"Dropping tables")
        drop_tables(target_db_url, target_schema, application)
        logger.info(f"Dropping columns")
        drop_columns(target_db_url, target_schema, application)
        phase_end = time.time()
        logger.info(f"Phase 3 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 4: Columns and Tables Renaming
    if "phase4" in phases_to_skip:
        logger.info("Skipping Phase 4: Columns and Tables Renaming")
    else:
        phase_start = time.time()
        logger.info(f"Phase 4: Columns and Tables Renaming")
        logger.info(f"Renaming columns")
        rename_columns(target_db_url, target_schema)
        logger.info(f"Renaming Tables")
        rename_tables(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 4 completed in {phase_end - phase_start:.2f} seconds")
    
    # Phase 5: Primary keys
    if "phase5" in phases_to_skip:
        logger.info("Skipping Phase 5: Primary keys")
    else:
        phase_start = time.time()
        logger.info(f"Phase 5: Primary keys")
        logger.info(f"Setting Primary keys")
        add_primary_keys(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 5 completed in {phase_end - phase_start:.2f} seconds")
    
    # Phase 6: Splitting columns
    if "phase6" in phases_to_skip:
        logger.info("Skipping Phase 6: Splitting Columns")
    else:
        phase_start = time.time()
        phase_end = time.time()
        logger.info("Phase 6: Splitting Columns")
        split_columns(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 6 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 7: Add Foreing Keys for Master Tables
    if "phase7" in phases_to_skip:
        logger.info("Skipping Phase 7: Add Foreing Keys for Master Tables")
    else:
        phase_start = time.time()
        logger.info(f"Phase 7: Add Foreing Keys for Master Tables")
        logger.info(f"Populating master tables - 7a")
        populate_master_tables(target_db_url, target_schema)
        logger.info(f"Implementing one to many relations - 7b")
        implement_one_to_many_relations(target_db_url, target_schema,csv_path="config/data_integrity_changes/phase7_one_to_many_relations.csv")
        logger.info(f"Implementing many to many relations - 7c")
        implement_many_to_many_relations(target_db_url, target_schema, csv_path="config/data_integrity_changes/phase7_many_to_many_relations.csv")
        logger.info(f"Dropping unused columns - 7d")
        drop_columns(target_db_url, target_schema, application, csv_path="config/data_integrity_changes/phase7_unused_columns.csv")
        phase_end = time.time()
        logger.info(f"Phase 7 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 8: Data Quality Changes
    if "phase8" in phases_to_skip:
        logger.info("Skipping Phase 8: Data Quality Changes")
    else:
        phase_start = time.time()
        logger.info(f"Phase 8: Data Quality Changes")
        logger.info(f"Changing Data Types - 8a")
        change_data_types(target_db_url, target_schema)
        logger.info(f"Applying Constraints - 8b")
        # apply_constraints(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 8 completed in {phase_end - phase_start:.2f} seconds")

    end_time = time.time()
    logger.info(f"Database sync process completed in {end_time - start_time:.2f} seconds for {application}")