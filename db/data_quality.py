import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger
from utils.utils import clean_column_for_datetime_conversion

logger = setup_logger()

def load_schema_changes(csv_path):
    """Helper function to load CSV changes."""
    return pd.read_csv(csv_path)
def change_data_types(target_db_url: str, schema_name: str, csv_path: str = "config/data_quality_changes/data_types.csv"):
    """
    Change column data types in a SQL Server database based on a CSV configuration.
    The 'new_data_type' from CSV directly dictates the target SQL type (e.g., DATE, DATETIME2).
    """
    try:
        dtypes_df = load_schema_changes(csv_path) # Ensure this function is defined
    except Exception as e:
        logger.error(f"Could not load schema changes from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        for _, row in dtypes_df.iterrows():
            table = str(row['table_name']).strip()
            column = str(row['column_name']).strip()
            old_type_from_csv = str(row['old_data_type']).strip()
            new_type_from_csv = str(row['new_data_type']).strip() # This is the target SQL type

            logger.debug(f"Processing {schema_name}.{table}.{column}: from CSV type {old_type_from_csv} to target SQL type {new_type_from_csv}")

            try:
                current_type_query = f"""
                    SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}';
                """
                db_debug = conn.execute(text(current_type_query)).fetchone()
                
                if db_debug is None:
                    logger.warning(f"Column {schema_name}.{table}.{column} does not exist. Skipping.")
                    continue
                
                current_db_type_name = db_debug[0]
                full_current_db_type = current_db_type_name # Base for constructing full type
                if current_db_type_name.lower() in ('varchar', 'nvarchar', 'char', 'nchar'):
                    length = db_debug[1] if db_debug[1] != -1 else 'MAX'
                    full_current_db_type = f"{current_db_type_name}({length})"
                elif current_db_type_name.lower() in ('decimal', 'numeric'):
                    full_current_db_type = f"{current_db_type_name}({db_debug[2]}, {db_debug[3]})"
                elif current_db_type_name.lower() in ('datetime2', 'datetimeoffset', 'time'):
                    full_current_db_type = f"{current_db_type_name}({db_debug[4]})"
                
                logger.debug(f"Current DB type for {schema_name}.{table}.{column} is {full_current_db_type}")

                actual_sql_alter_type = new_type_from_csv # The target type is taken directly from CSV

                string_like_old_types = ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext')
                is_old_type_string_like = any(old_type_from_csv.lower().startswith(s_type) for s_type in string_like_old_types)

                if is_old_type_string_like:
                    target_new_type_category = new_type_from_csv.lower().split('(')[0] # Get base type like 'date', 'datetime2'

                    if target_new_type_category == 'bit':
                        logger.debug(f"Cleaning {schema_name}.{table}.{column} for BIT conversion.")
                        clean_data_sql = f"""
                            UPDATE {schema_name}.{table} SET {column} = NULL
                            WHERE LTRIM(RTRIM(CAST({column} AS VARCHAR(MAX)))) = '' OR 
                                  (CAST({column} AS VARCHAR(MAX)) NOT IN ('0', '1') AND {column} IS NOT NULL);
                        """
                        conn.execute(text(clean_data_sql))
                    
                    elif target_new_type_category in ('float', 'real', 'int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'money', 'smallmoney'):
                        logger.debug(f"Cleaning {schema_name}.{table}.{column} for numeric conversion to {new_type_from_csv.upper()}.")
                        clean_empty_sql = f"""UPDATE {schema_name}.{table} SET {column} = NULL WHERE LTRIM(RTRIM(CAST({column} AS VARCHAR(MAX)))) = '';"""
                        conn.execute(text(clean_empty_sql))
                        clean_non_numeric_sql = f"""
                            UPDATE {schema_name}.{table} SET {column} = NULL
                            WHERE TRY_CONVERT({new_type_from_csv}, {column}) IS NULL AND {column} IS NOT NULL;
                        """
                        conn.execute(text(clean_non_numeric_sql))
                    
                    elif target_new_type_category in ('date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset'):
                        logger.debug(f"Cleaning {schema_name}.{table}.{column} for explicit datetime conversion to {new_type_from_csv.upper()}.")
                        clean_column_for_datetime_conversion(conn, schema_name, table, column)
                        
                        # Final clean specifically for the target type from CSV
                        final_clean_non_convertible_sql = f"""
                            UPDATE {schema_name}.{table} SET {column} = NULL
                            WHERE {column} IS NOT NULL AND TRY_CONVERT({new_type_from_csv}, {column}) IS NULL;
                        """
                        conn.execute(text(final_clean_non_convertible_sql))
                        # actual_sql_alter_type is already new_type_from_csv
                    else:
                        logger.debug(f"No specific string cleaning logic for target type {new_type_from_csv} from {old_type_from_csv}.")
                
                # Skip if current DB type (full definition) already matches the target SQL type from CSV
                if actual_sql_alter_type.lower() == full_current_db_type.lower():
                    logger.debug(f"Column {schema_name}.{table}.{column} is already exactly of type {actual_sql_alter_type} (full DB definition: {full_current_db_type}). Skipping ALTER COLUMN.")
                    continue
                # Add a nuanced log if base types match but full definitions differ (e.g. CSV says 'VARCHAR', DB is 'VARCHAR(50)')
                # This indicates the ALTER will proceed, which is usually intended if the CSV specifies a change (e.g. to VARCHAR(MAX) or different precision)
                elif current_db_type_name.lower() == actual_sql_alter_type.lower().split('(')[0] and \
                     full_current_db_type.lower() != actual_sql_alter_type.lower():
                     logger.debug(f"Base type for {schema_name}.{table}.{column} ({current_db_type_name}) matches target base {actual_sql_alter_type.split('(')[0]}. "
                                 f"However, full DB type is {full_current_db_type} and target is {actual_sql_alter_type}. Proceeding with ALTER.")


                logger.debug(f"Attempting to alter {schema_name}.{table}.{column} to type {actual_sql_alter_type}.")
                alter_sql = f"""
                    ALTER TABLE {schema_name}.{table}
                    ALTER COLUMN {column} {actual_sql_alter_type};
                """
                conn.execute(text(alter_sql))
                logger.debug(f"Successfully changed type of {schema_name}.{table}.{column} to {actual_sql_alter_type}.")

            except Exception as e:
                logger.error(f"Failed to process column {schema_name}.{table}.{column}. Error: {e}")
                raise 
        
        logger.debug("All schema changes processed successfully.")

def apply_constraints(target_db_url, schema_name, csv_path="config/data_quality_changes/constraints.csv"):
    """
    Apply constraints (MAX, MIN, UNIQUE) based on constraints.csv.
    Columns: table_name, column_name, constraint_type, value
    """
    constraints_df = load_schema_changes(csv_path)

    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in constraints_df.iterrows():
            table = row['table_name']
            column = row['column_name']
            constraint_type = row['constraint_type'].upper()
            value = row.get('value')  # May be NaN for UNIQUE

            try:
                if constraint_type == "MAX":
                    sql = f"""
                        ALTER TABLE {schema_name}.{table}
                        ADD CONSTRAINT CK_{table}_{column}_max CHECK ({column} <= {value});
                    """
                elif constraint_type == "MIN":
                    sql = f"""
                        ALTER TABLE {schema_name}.{table}
                        ADD CONSTRAINT CK_{table}_{column}_min CHECK ({column} >= {value});
                    """
                elif constraint_type == "UNIQUE":
                    sql = f"""
                        ALTER TABLE {schema_name}.{table}
                        ADD CONSTRAINT UQ_{table}_{column} UNIQUE ({column});
                    """
                else:
                    logger.warning(f"Unknown constraint type '{constraint_type}' for {schema_name}.{table}.{column}")
                    continue

                conn.execute(text(sql))
                logger.debug(f"Applied {constraint_type} constraint on {schema_name}.{table}.{column}")
            except Exception as e:
                logger.error(f"Failed to apply constraint on {schema_name}.{table}.{column}: {e}")
                raise