from config.constants import APPS_TO_SKIP, PHASES_TO_SKIP
from config.db_config import build_connection_url, load_config
from engine.sync_engine import sync_databases
from utils.logger import setup_logger

if __name__ == "__main__":
    # Set up the logger
    logger = setup_logger()

    logger.info("Starting Program....")
    # Load the configuration from config.json
    config = load_config("config.json")

    for app in ['pulse','insights']:
        if app in APPS_TO_SKIP:
            logger.info(f"Skipping {app}.")
            continue
        # Get source and target database configurations for pulse
        source_config = config[f"{app}_source_db"]
        target_config = config[f"{app}_target_db"]
        source_schema = config[f'{app}_source_db']['schema']
        target_schema = config[f'{app}_target_db']['schema']

        # Build connection URLs
        source_db_url = build_connection_url(source_config)
        target_db_url = build_connection_url(target_config)

        phase_list = [f"phase{num}" for num in PHASES_TO_SKIP]
        sync_databases(source_db_url, target_db_url, source_schema, target_schema, application=app, phases_to_skip=phase_list)
    
    logger.info("Closing Program....")