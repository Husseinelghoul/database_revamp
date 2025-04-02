import logging

def setup_logger(log_file="sync_tool.log"):
    """
    Sets up the logger for the sync tool to log to both console and file.
    The log file is overwritten each time the script runs.
    :param log_file: Path to the log file.
    :return: Configured logger instance.
    """
    logger = logging.getLogger("sync_tool")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()  # Clear any existing handlers

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger