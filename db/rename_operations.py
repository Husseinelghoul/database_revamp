from sqlalchemy import create_engine, text

from utils.logger import setup_logger
from utils.utils import load_schema_changes

logger = setup_logger()

def rename_columns(target_db_url, schema_name, csv_path="config/schema_changes/column_renames.csv"):
    """Rename columns specified in the CSV file."""
    renames_df = load_schema_changes(csv_path)
    
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in renames_df.iterrows():
            try:
                conn.execute(text(f"EXEC sp_rename '{schema_name}.{row['table_name']}.{row['column_old_name']}', '{row['column_new_name']}', 'COLUMN'"))
                logger.debug(f"Renamed column {row['column_old_name']} to {row['column_new_name']} in {schema_name}.{row['table_name']}")
            except Exception as e:
                logger.error(f"Failed to rename column {row['column_old_name']} to {row['column_new_name']} in {schema_name}.{row['table_name']}: {e}")
                raise e

def rename_tables(target_db_url, schema_name, csv_path="config/schema_changes/table_renames.csv"):
    """Rename tables specified in the CSV file."""
    renames_df = load_schema_changes(csv_path)
    
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in renames_df.iterrows():
            try:
                conn.execute(text(f"EXEC sp_rename '{schema_name}.{row['old_table_name']}', '{row['new_table_name']}'"))
                logger.debug(f"Renamed table {schema_name}.{row['old_table_name']} to {schema_name}.{row['new_table_name']}")
            except Exception as e:
                logger.error(f"Failed to rename table {schema_name}.{row['old_table_name']} to {schema_name}.{row['new_table_name']}: {e}")
                raise e