import json
import urllib.parse

from config.constants import obcd_driver_version


def load_config(config_path="config.json"):
    """
    Load the database configuration from a JSON file.
    :param config_path: Path to the config.json file.
    :return: Dictionary containing source and target database configurations.
    """
    with open(config_path, "r") as file:
        config = json.load(file)
    return config

def build_connection_url(config):
    """
    Build the SQLAlchemy connection URL for MSSQL.
    :param config: Dictionary containing database connection details.
    :return: SQLAlchemy connection URL string.
    """
    encoded_password = urllib.parse.quote_plus(config["password"])
    return f"mssql+pyodbc://{config['username']}:{encoded_password}@{config['server']}/{config['database']}?driver=ODBC+Driver+{obcd_driver_version}+for+SQL+Server"