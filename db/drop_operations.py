from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from utils.logger import setup_logger
from utils.utils import load_schema_changes

logger = setup_logger()

def drop_tables(target_db_url: str, schema_name: str, application: str, csv_path="config/schema_changes/table_drops.csv"):
    """
    Optimized function to drop tables. It gathers a complete list of tables to drop
    from both the CSV and name matching BEFORE executing any DROP commands.
    """
    engine = create_engine(target_db_url)
    tables_to_drop = set()

    # --- READ PHASE (No transaction needed) ---

    # Part 1: Get tables to drop from the CSV file
    logger.debug(f"Reading tables to drop for application '{application}' from CSV.")
    try:
        drops_df = load_schema_changes(csv_path)
        if not drops_df.empty:
            app_drops_df = drops_df[drops_df["database"].isin([application, 'both'])]
            for table_name in app_drops_df['table_name']:
                tables_to_drop.add(table_name)
    except Exception as e:
        logger.error(f"An error occurred processing the CSV file '{csv_path}': {e}")

    # Part 2: Get tables to drop based on name matching
    logger.debug(f"Querying schema '{schema_name}' for tables with '2025' or 'bkp' in their name.")
    try:
        inspector = inspect(engine)
        all_tables = inspector.get_table_names(schema=schema_name)
        
        for table_name in all_tables:
            if '2025' in table_name or 'bkp' in table_name.lower():
                tables_to_drop.add(table_name)
    except Exception as e:
        logger.error(f"Failed to inspect schema '{schema_name}' for tables to drop by name: {e}")
        # We can continue with just the CSV list if inspection fails
    
    # --- WRITE PHASE (Single transaction) ---

    if not tables_to_drop:
        logger.debug("No tables to drop after checking CSV and name patterns.")
        return

    logger.debug(f"Final list of tables to be dropped: {sorted(list(tables_to_drop))}")
    
    with engine.begin() as conn:
        for table_name in tables_to_drop:
            try:
                # Use IF EXISTS for safety
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}"'))
                logger.debug(f"SUCCESS: Dropped table: {schema_name}.{table_name}")
            except SQLAlchemyError as e:
                # This will catch other errors like permission issues
                logger.error(f"FAILED to drop table {schema_name}.{table_name}: {e}")
                continue # Continue to the next table


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

