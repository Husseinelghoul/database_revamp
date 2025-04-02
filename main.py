from config.db_config import load_config, build_connection_url
from engine.sync_engine import sync_databases
from utils.logger import setup_logger

if __name__ == "__main__":
    # Set up the logger
    logger = setup_logger()

    logger.info("Starting Program....")
    # Load the configuration from config.json
    config = load_config("config.json")

    # Get source and target database configurations
    source_config = config["source_db"]
    target_config = config["target_db"]

    # Build connection URLs
    source_db_url = build_connection_url(source_config)
    target_db_url = build_connection_url(target_config)

    sync_databases(source_db_url, target_db_url, config)
