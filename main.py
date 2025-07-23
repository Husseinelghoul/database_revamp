import sys
from config.constants import PHASES_TO_SKIP
from config.db_config import build_connection_url, load_config
from engine.sync_engine import sync_databases
from utils.logger import setup_logger

if __name__ == "__main__":
    # Set up the logger
    logger = setup_logger()

    # === Read app name from command line ===
    # Check if an application name was provided
    if len(sys.argv) < 2:
        logger.error("Error: No application name provided on the command line.")
        logger.info("Usage: python your_script_name.py <app_name> (e.g., pulse or insights)")
        sys.exit(1) # Exit if no app name is given
    if sys.argv[1] not in ('pulse','insights'):
        logger.error("Error: Wrong application name provided on the command line.")
        logger.info("Usage: python your_script_name.py <app_name> (e.g., pulse or insights)")
        sys.exit(1) # Exit if no app name is given
    

    # Get the application name from the first command-line argument
    app = sys.argv[1]
    logger.info(f"Starting Program for application: {app}")
    # ========================================================

    # Load the configuration from config.json
    config = load_config("config.json")

    # The FOR loop and the IF check for APPS_TO_SKIP are now removed.
    # The script will run only for the 'app' provided.
    try:
        # Get source and target database configurations
        source_config = config[f"{app}_source_db"]
        target_config = config[f"{app}_target_db"]
        source_schema = config[f'{app}_source_db']['schema']
        target_schema = config[f'{app}_target_db']['schema']
    except KeyError:
        logger.error(f"Error: Configuration for application '{app}' not found in config.json.")
        sys.exit(1)

    # Build connection URLs
    source_db_url = build_connection_url(source_config)
    target_db_url = build_connection_url(target_config)

    phase_list = [f"phase{num}" for num in PHASES_TO_SKIP]
    sync_databases(source_db_url, target_db_url, source_schema, target_schema, application=app, phases_to_skip=phase_list)
    
    logger.info("Closing Program....")