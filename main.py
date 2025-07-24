import sys
from config.constants import PHASES_TO_SKIP
from config.db_config import build_connection_url, load_config
from engine.sync_engine import sync_databases
from utils.logger import setup_logger

def run_sync_for_app(app, logger, config):
    """
    Runs the database synchronization process for a single application.

    This function encapsulates the logic for fetching configuration, building
    database URLs, and initiating the sync for a specific app.

    Args:
        app (str): The name of the application to sync (e.g., 'pulse' or 'insights').
        logger: The configured logger instance for logging messages.
        config (dict): The loaded configuration dictionary.
    """
    logger.info(f"--- Starting sync for application: {app} ---")
    
    # Get source and target database configurations for the given app
    try:
        source_config = config[f"{app}_source_db"]
        target_config = config[f"{app}_target_db"]
        source_schema = config[f'{app}_source_db']['schema']
        target_schema = config[f'{app}_target_db']['schema']
    except KeyError:
        logger.error(f"Error: Configuration for application '{app}' not found in config.json.")
        return # Skip this app and allow the script to continue with the next one

    # Build connection URLs from the configuration details
    source_db_url = build_connection_url(source_config)
    target_db_url = build_connection_url(target_config)

    # Define which phases of the sync to skip
    phase_list = [f"phase{num}" for num in PHASES_TO_SKIP]
    
    # Execute the core database synchronization logic
    sync_databases(source_db_url, target_db_url, source_schema, target_schema, application=app, phases_to_skip=phase_list)
    
    logger.info(f"--- Finished sync for application: {app} ---")


if __name__ == "__main__":
    # Set up the logger for the script
    logger = setup_logger()

    # === Read and validate the command-line argument ===
    if len(sys.argv) < 2:
        logger.error("Error: No application name provided on the command line.")
        logger.info(f"Usage: python {sys.argv[0]} <app_name>")
        logger.info("       <app_name> can be 'pulse', 'insights', or 'both'.")
        sys.exit(1)

    # Use the first command-line argument and convert to lowercase for case-insensitivity
    app_argument = sys.argv[1].lower()
    valid_arguments = ['pulse', 'insights', 'both']

    if app_argument not in valid_arguments:
        logger.error(f"Error: Invalid argument '{app_argument}'.")
        logger.info(f"Usage: python {sys.argv[0]} <app_name>")
        logger.info(f"       <app_name> must be one of: {', '.join(valid_arguments)}.")
        sys.exit(1)
    
    # === Determine which applications to run based on the argument ===
    apps_to_run = []
    if app_argument == 'both':
        # If 'both', ensure 'pulse' runs before 'insights'
        apps_to_run = ['pulse', 'insights']
    else:
        # Otherwise, just run the single specified application
        apps_to_run = [app_argument]
    
    logger.info(f"Starting Program. Will process: {', '.join(apps_to_run)}")
    
    # Load the configuration once to be passed to the function
    config = load_config("config.json")

    # === Loop through and run the sync for each required application ===
    for app in apps_to_run:
        try:
            run_sync_for_app(app, logger, config)
        except Exception as e:
            # Catch any unexpected errors during an app's sync process
            logger.error(f"An unexpected error occurred while processing '{app}': {e}", exc_info=True)
            logger.info(f"Continuing to the next application if any are remaining.")
            
    logger.info("All requested processes are complete. Closing Program....")
