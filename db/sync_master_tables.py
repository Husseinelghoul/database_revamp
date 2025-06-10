
from config.db_config import build_connection_url, load_config
from db.data_migrator import migrate_data
from db.schema_writer import read_schema, write_schema
from utils.logger import setup_logger

logger = setup_logger()

config = load_config("config.json")

pulse_source_schema = config[f'pulse_source_db']['schema']
insights_target_schema = config[f'insights_target_db']['schema']

# Build connection URLs
pulse_db_url = build_connection_url(config["pulse_source_db"])
insights_db_url = build_connection_url(config[f"insights_target_db"])


def sync_master_tables():
    schema, tables = read_schema(pulse_db_url, pulse_source_schema)
    filtered_schema = {key: value for key, value in schema.items() if "master" in key.lower()}
    filtered_tables = {key: value for key, value in tables.items() if 'master' in key.lower()}
    logger.debug(f"Copying {len(filtered_tables)} tables schema...")
    write_schema(insights_db_url, filtered_tables, insights_target_schema)
    logger.info("Completed copying master tables schema to insights")
    logger.info("Copying master tables data")
    migrate_data(pulse_db_url, insights_db_url, filtered_schema, pulse_source_schema, insights_target_schema)
    logger.info("Completed copying master tables data")
    