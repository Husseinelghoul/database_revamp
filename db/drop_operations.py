from sqlalchemy import create_engine, text

from utils.logger import setup_logger
from utils.utils import load_schema_changes

logger = setup_logger()

def drop_tables(target_db_url, schema_name, application: str, csv_path="config/schema_changes/table_drops.csv"):
    """Drop tables specified in the CSV file."""
    drops_df = load_schema_changes(csv_path)
    drops_df = drops_df[drops_df["database"] == application]
    if drops_df.empty:
        logger.info(f"No tables to drop for {application}.")
        return
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in drops_df.iterrows():
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{row['table_name']}"))
                logger.debug(f"Dropped table {schema_name}.{row['table_name']}")
            except Exception as e:
                logger.error(f"Failed to drop table {schema_name}.{row['table_name']}: {e}")

def drop_columns(target_db_url, schema_name, application: str, csv_path="config/schema_changes/column_drops.csv"):
    """Drop columns specified in the CSV file."""
    drops_df = load_schema_changes(csv_path)
    drops_df = drops_df[drops_df["database"] == application]
    if drops_df.empty:
        logger.info(f"No columns to drop for {application}.")
        return
    
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in drops_df.iterrows():
            try:
                conn.execute(text(f"ALTER TABLE {schema_name}.{row['table_name']} DROP COLUMN IF EXISTS {row['column_name']}"))
                logger.debug(f"Dropped column {row['column_name']} from {schema_name}.{row['table_name']}")
            except Exception as e:
                logger.error(f"Failed to drop column {row['column_name']} from {schema_name}.{row['table_name']}: {e}")

