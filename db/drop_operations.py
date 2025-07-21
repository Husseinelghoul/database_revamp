from sqlalchemy import create_engine, inspect, text

from utils.logger import setup_logger
from utils.utils import load_schema_changes

logger = setup_logger()

def drop_tables(target_db_url: str, schema_name: str, application: str, csv_path="config/schema_changes/table_drops.csv"):
    """
    Drop tables specified in the CSV and tables with '2025' or 'bkp' in their name.
    """
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        # Part 1: Drop tables specified in the CSV file (original logic)
        logger.info(f"Starting to drop tables for application '{application}' from CSV.")
        try:
            drops_df = load_schema_changes(csv_path)
            drops_df = drops_df[drops_df["database"].isin([application, 'both'])]
            
            if drops_df.empty:
                logger.info(f"No tables to drop from CSV for '{application}'.")
            else:
                for _, row in drops_df.iterrows():
                    table_name_csv = row['table_name']
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name_csv}"))
                        logger.debug(f"Dropped table from CSV: {schema_name}.{table_name_csv}")
                    except Exception as e:
                        logger.error(f"Failed to drop table {schema_name}.{table_name_csv}: {e}")
        except Exception as e:
            logger.error(f"An error occurred processing the CSV file '{csv_path}': {e}")

        # Part 2: Get all tables from the schema and drop based on name matching
        logger.info(f"Checking for tables with '2025' or 'bkp' in their name in schema '{schema_name}'.")
        try:
            # Create an inspector object to query database metadata
            inspector = inspect(engine)
            
            # Get all table names from the specified schema
            all_tables = inspector.get_table_names(schema=schema_name)
            
            # Filter tables if '2025' is in the name or 'bkp' (case-insensitive)
            tables_to_drop_by_name = [
                tbl for tbl in all_tables
                if '2025' in tbl or 'bkp' in tbl.lower()
            ]

            if not tables_to_drop_by_name:
                logger.info("No tables found with '2025' or 'bkp' in their name.")
            else:
                logger.info(f"Found tables to drop by name: {tables_to_drop_by_name}")
                for table_name in tables_to_drop_by_name:
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name}"))
                        logger.debug(f"Dropped table by name match: {schema_name}.{table_name}")
                    except Exception as e:
                        logger.error(f"Failed to drop table {schema_name}.{table_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to inspect or drop tables by name in schema '{schema_name}': {e}")


def drop_columns(target_db_url, schema_name, application: str, csv_path="config/schema_changes/column_drops.csv"):
    """Drop columns specified in the CSV file."""
    drops_df = load_schema_changes(csv_path)
    if "database" in drops_df.columns:
        drops_df = drops_df[drops_df["database"] == application]
    if drops_df.empty:
        logger.debug(f"No columns to drop for {application}.")
        return
    
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in drops_df.iterrows():
            try:
                conn.execute(text(f"ALTER TABLE {schema_name}.{row['table_name']} DROP COLUMN IF EXISTS {row['column_name']}"))
                logger.debug(f"Dropped column {row['column_name']} from {schema_name}.{row['table_name']}")
            except Exception as e:
                logger.error(f"Failed to drop column {row['column_name']} from {schema_name}.{row['table_name']}: {e}")

