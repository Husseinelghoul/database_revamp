import time

from config.db_config import build_connection_url, load_config
from db.create_lookup_media_tables import (create_lookup_project_to_media,
                                           create_media_lookup)
from db.custom_operations import (
    create_lookup_project_to_project_phase_category,
    implement_predecessor_successor, link_project_management_to_sources,
    merge_project_status_columns, update_master_entity_full_name)
from db.data_integrity import (add_primary_keys,
                               implement_many_to_many_relations,
                               implement_one_to_many_relations)
from db.data_migrator import migrate_data
from db.data_quality import (apply_data_quality_rules, change_data_types,
                             set_health_safety_default)
from db.database_optimization import create_project_period_indexes
from db.drop_operations import drop_columns, drop_tables
from db.rename_operations import rename_columns, rename_tables
from db.schema_changes import merge_milestone_status, populate_master_roles_for_contract_vo_status
from db.schema_writer import read_schema, replicate_schema_with_sql
from db.sync_master_tables import (merge_master_tables, populate_master_tables,
                                   streamlined_sync_master_tables)
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
            filtered_tables = {
                    key: value for key, value in tables.items()
                    if key.lower() == 'master_project_to_project_phase' or 'master' not in key.lower()
                }
            tables = filtered_tables
        replicate_schema_with_sql(source_db_url, target_db_url, source_schema, target_schema)
        if application == "insights":
            logger.info("Writing master tables into insights")
            config = load_config("config.json")
            pulse_source_schema = config[f'pulse_source_db']['schema']
            pulse_db_url = build_connection_url(config["pulse_source_db"])
            streamlined_sync_master_tables(pulse_db_url, target_db_url, pulse_source_schema, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 1 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 2: Data Duplication
    if "phase2" in phases_to_skip:
        logger.info("Skipping Phase 2: Data Duplication")
    else:
        logger.info("Phase 2: Migrating data to target database...")
        phase_start = time.time()
        if application == "insights":
            filtered_schema = {
                    key: value for key, value in schema.items() 
                    if "master" not in key.lower() or key.lower() == 'master_project_to_project_phase'
                }
            schema = filtered_schema
        migrate_data(source_db_url, target_db_url, schema, source_schema, target_schema,
                        project_names=None, periods=None)
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

    # Phase 5: Splitting columns and tables
    if "phase5" in phases_to_skip:
        logger.info("Skipping Phase 5: Splitting Columns")
    else:
        phase_start = time.time()
        phase_end = time.time()
        logger.info("Phase 5: Splitting and merging")
        logger.info("Splitting Columns - 5a")
        populate_master_roles_for_contract_vo_status(target_db_url, target_schema)
        logger.info("Merging milestone_status column")
        merge_milestone_status(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 5 completed in {phase_end - phase_start:.2f} seconds")
    # Phase 6: Primary keys
    if "phase6" in phases_to_skip:
        logger.info("Skipping Phase 6: Primary keys")
    else:
        phase_start = time.time()
        logger.info(f"Phase 6: Primary keys")
        logger.info(f"Setting Primary keys")
        add_primary_keys(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 6 completed in {phase_end - phase_start:.2f} seconds")
    # Phase 7: Data Quality Changes
    if "phase7" in phases_to_skip:
        logger.info("Skipping Phase 7: Data Quality Changes")
    else:
        phase_start = time.time()
        logger.info(f"Phase 7: Data Quality Changes")
        logger.info(f"Changing Data Types - 7a")
        change_data_types(target_db_url, target_schema)
        logger.info("Applying other data quality rules - 7a")
        set_health_safety_default(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 7 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 8 Custom changes
    if "phase8" in phases_to_skip:
        logger.info("Skipping Phase 8: Custom changes")
    else:
        phase_start = time.time()
        logger.info(f"Implement predecessor-successor - 8a")
        implement_predecessor_successor(target_db_url, target_schema)
        logger.info(f"Implement media mapping - 8b")
        create_lookup_project_to_media(target_db_url, target_schema)
        create_media_lookup(target_db_url, target_schema)
        logger.info(f"Link project management to sources - 8c")
        link_project_management_to_sources(target_db_url, target_schema)
        logger.info(f"Create and populate lookup_project_to_project_phase_category - 8d")
        create_lookup_project_to_project_phase_category(target_db_url, target_schema)
        logger.info(f"Merging project status table columns - 8d")
        merge_project_status_columns(target_db_url, target_schema)
        logger.info(f"Populate master_entity table - 8e")
        update_master_entity_full_name(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 8 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 9: Add Foreing Keys for Master Tables
    if "phase9" in phases_to_skip:
        logger.info("Skipping Phase 9: Add Foreing Keys for Master Tables")
    else:
        phase_start = time.time()
        logger.info(f"Phase 9: Add Foreing Keys for Master Tables")
        logger.info(f"Populating master tables - 9a")
        populate_master_tables(target_db_url, target_schema)
        logger.info(f"Implementing one to many relations - 9b")
        implement_one_to_many_relations(target_db_url, target_schema)
        logger.info(f"Implementing many to many relations - 9c")
        implement_many_to_many_relations(target_db_url, target_schema)
        logger.info(f"Dropping unused columns - 9d")
        drop_columns(target_db_url, target_schema, application, csv_path="config/data_integrity_changes/unused_columns.csv")
        phase_end = time.time()
        logger.info(f"Phase 9 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 10: Implementing Indexes
    if "phase10" in phases_to_skip:
        logger.info("Skipping Phase 10: Implementing Indexes")
    else:
        phase_start = time.time()
        logger.info(f"Phase 10: Implementing Indexes")
        create_project_period_indexes(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 10 completed in {phase_end - phase_start:.2f} seconds")

    # Phase 11: Data Quality Rules
    if "phase11" in phases_to_skip:
        logger.info("Skipping Phase 11: Data Quality Rules")
    else:
        phase_start = time.time()
        apply_data_quality_rules(target_db_url, target_schema)
        phase_end = time.time()
        logger.info(f"Phase 11 completed in {phase_end - phase_start:.2f} seconds")
        
    end_time = time.time()
    logger.info(f"Database sync process completed in {end_time - start_time:.2f} seconds for {application}")