from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from utils.logger import setup_logger

logger = setup_logger()


# --- Helper Functions ---
def load_schema_changes(csv_path: str) -> pd.DataFrame:
    """Helper function to load CSV changes, returning an empty DataFrame if file not found."""
    try:
        return pd.read_csv(csv_path)
    except FileNotFoundError:
        logger.warning(f"Configuration file not found at: {csv_path}. Skipping this operation.")
        return pd.DataFrame()

def get_db_schema_info(engine, schema_name: str) -> dict:
    """
    Efficiently fetches all table and column names for a given schema in a single query.
    Returns a dictionary with all names converted to lowercase for case-insensitive matching.
    Structure: {'table1': {'col1_original_case': 'col1', 'col2_original_case': 'col2'}, ...}
    """
    logger.debug(f"Fetching schema information for '{schema_name}'...")
    query = text("""
        SELECT TABLE_NAME, COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
    """)
    # schema_info will store: { 'lowercase_table_name': { 'lowercase_col_name': 'OriginalCaseColName' } }
    schema_info = defaultdict(dict)
    try:
        with engine.connect() as conn:
            results = conn.execute(query, {"schema": schema_name})
            for table, column in results:
                schema_info[table.lower()][column.lower()] = column
        logger.debug(f"Successfully fetched schema for {len(schema_info)} tables.")
        return dict(schema_info)
    except SQLAlchemyError as e:
        logger.error(f"Failed to fetch database schema info: {e}")
        return {}


# --- Optimized Core Functions ---

def rename_columns(target_db_url: str, schema_name: str, csv_path: str = "config/schema_changes/column_renames.csv"):
    """
    Optimized function to rename columns. It reads all renames, groups them by table,
    and checks for existence case-insensitively before executing.
    """
    renames_df = load_schema_changes(csv_path)
    if renames_df.empty:
        return

    engine = create_engine(target_db_url)
    db_schema = get_db_schema_info(engine, schema_name)
    
    with engine.begin() as conn:
        # Group changes by table for more efficient processing
        for table_name_from_csv, group in renames_df.groupby('table_name'):
            table_name_lower = table_name_from_csv.lower()
            
            # Check if the table itself exists (case-insensitively)
            if table_name_lower not in db_schema:
                logger.debug(f"Table '{schema_name}.{table_name_from_csv}' not found. Skipping all column renames for it.")
                continue

            # Get the column mapping for this specific table: { 'lowercase_name': 'OriginalCaseName' }
            table_columns_map = db_schema[table_name_lower]
            
            for _, row in group.iterrows():
                old_col_from_csv = row['column_old_name']
                new_col = row['column_new_name']
                
                # Now check if the specific column exists in this table (case-insensitively)
                if old_col_from_csv.lower() not in table_columns_map:
                    logger.debug(f"Column '{old_col_from_csv}' not found in table '{schema_name}.{table_name_from_csv}'. Skipping rename.")
                    continue

                # Get the original, correct-cased column name for the SQL command
                original_old_col_name = table_columns_map[old_col_from_csv.lower()]

                try:
                    rename_sql = text("EXEC sp_rename :old_name, :new_name, 'COLUMN'")
                    params = {
                        "old_name": f"{schema_name}.{table_name_from_csv}.{original_old_col_name}",
                        "new_name": new_col
                    }
                    conn.execute(rename_sql, params)
                    logger.debug(f"SUCCESS: Renamed column '{original_old_col_name}' to '{new_col}' in '{schema_name}.{table_name_from_csv}'")
                except SQLAlchemyError as e:
                    logger.error(f"FAILED to rename column '{original_old_col_name}' in '{schema_name}.{table_name_from_csv}': {e}")
                    continue


def rename_tables(target_db_url: str, schema_name: str, csv_path: str = "config/schema_changes/table_renames.csv"):
    """
    Optimized function to rename tables. It fetches existing tables first to avoid
    errors when trying to rename a non-existent table.
    """
    renames_df = load_schema_changes(csv_path)
    if renames_df.empty:
        return
        
    engine = create_engine(target_db_url)
    # Get just the lowercase table names for checking existence
    db_tables_lower = get_db_schema_info(engine, schema_name).keys()

    with engine.begin() as conn:
        for _, row in renames_df.iterrows():
            old_table = row['old_table_name']
            new_table = row['new_table_name']

            # Check if the source table exists (case-insensitively)
            if old_table.lower() not in db_tables_lower:
                logger.debug(f"Table '{schema_name}.{old_table}' not found. Skipping rename to '{new_table}'.")
                continue

            try:
                rename_sql = text("EXEC sp_rename :old_name, :new_name")
                params = {
                    "old_name": f"{schema_name}.{old_table}",
                    "new_name": new_table
                }
                conn.execute(rename_sql, params)
                logger.debug(f"SUCCESS: Renamed table '{schema_name}.{old_table}' to '{schema_name}.{new_table}'")
            except SQLAlchemyError as e:
                logger.error(f"FAILED to rename table '{schema_name}.{old_table}': {e}")
                continue
