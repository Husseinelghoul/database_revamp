from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

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

def _drop_existing_constraints(conn, schema_name):
    """
    Finds and drops all existing CHECK and UNIQUE constraints in the schema.
    This ensures a clean slate before applying new rules.
    """
    logger.info(f"Dropping all existing CHECK and UNIQUE constraints in schema '{schema_name}'...")
    
    # Query to find all constraints to drop (excluding Foreign Keys and Primary Keys)
    constraints_to_drop_query = text("""
        SELECT TABLE_NAME, CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = :schema
          AND CONSTRAINT_TYPE IN ('CHECK', 'UNIQUE');
    """)
    
    try:
        constraints = conn.execute(constraints_to_drop_query, {"schema": schema_name}).fetchall()
        if not constraints:
            logger.info("No existing CHECK or UNIQUE constraints to drop.")
            return

        logger.debug(f"Found {len(constraints)} constraints to drop.")
        for table, constraint_name in constraints:
            try:
                drop_sql = f'ALTER TABLE "{schema_name}"."{table}" DROP CONSTRAINT "{constraint_name}";'
                conn.execute(text(drop_sql))
                logger.debug(f"Dropped constraint '{constraint_name}' from table '{table}'.")
            except SQLAlchemyError as e:
                logger.warning(f"Could not drop constraint '{constraint_name}' from '{table}'. It might be in use. Error: {e.orig}")
        logger.info("Finished dropping existing constraints.")
    except Exception as e:
        logger.error(f"Failed during the constraint dropping phase: {e}")
        raise

def _apply_automatic_date_rules(conn, schema_name):
    """
    Finds all date/datetime columns and applies a MIN YEAR 2000 CHECK constraint.
    """
    logger.info("Applying automatic date constraints (MIN YEAR 2000)...")
    date_cols_query = text("""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND DATA_TYPE IN ('date', 'datetime', 'datetime2', 'smalldatetime')
          AND COLUMN_NAME NOT IN ('last_updated');
    """)
    date_columns_to_check = conn.execute(date_cols_query, {"schema": schema_name}).fetchall()

    for table, column in date_columns_to_check:
        full_column_path = f'"{schema_name}"."{table}"."{column}"'
        
        find_old_dates_sql = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table}" WHERE "{column}" < \'2000-01-01\';')
        if conn.execute(find_old_dates_sql).scalar() > 0:
            logger.warning(f"Found dates before 2000 in {full_column_path}. Setting them to NULL.")
            clean_sql = text(f'UPDATE "{schema_name}"."{table}" SET "{column}" = NULL WHERE "{column}" < \'2000-01-01\';')
            conn.execute(clean_sql)

        constraint_name = f"CK_{table}_{column}_min_year_2000"
        add_constraint_sql = text(f"""
            ALTER TABLE "{schema_name}"."{table}"
            ADD CONSTRAINT "{constraint_name}" CHECK ("{column}" IS NULL OR "{column}" >= '2000-01-01');
        """)
        try:
            conn.execute(add_constraint_sql)
            logger.debug(f"SUCCESS: Applied MIN YEAR 2000 constraint to {full_column_path}")
        except SQLAlchemyError as e:
            # This logic is now handled by the _drop_existing_constraints function,
            # but we keep this as a safeguard against race conditions or other issues.
            if "already exists" in str(e).lower():
                 logger.debug(f"Constraint '{constraint_name}' already exists on {full_column_path}. This should have been dropped; skipping.")
            else:
                logger.warning(f"Could not apply date constraint to {full_column_path}. Reason: {e.orig}")

def _clean_data_for_table(conn, schema_name, table, rules):
    """Builds and executes a single bulk UPDATE statement to clean data for one table."""
    update_conditions = defaultdict(list)
    for rule in rules:
        column_name = str(rule['column_name']).strip()
        constraint_type = str(rule['constraint_type']).upper()
        value = rule.get('value')

        if ";" in column_name or pd.isna(value): continue

        if constraint_type == "MAX":
            update_conditions[column_name].append(f'"{column_name}" > {value}')
        elif constraint_type == "MIN":
            update_conditions[column_name].append(f'"{column_name}" < {value}')
    
    if update_conditions:
        set_clauses = []
        for col, conditions in update_conditions.items():
            full_condition = " OR ".join(conditions)
            set_clauses.append(f'"{col}" = CASE WHEN {full_condition} THEN NULL ELSE "{col}" END')
        
        clean_sql = f'UPDATE "{schema_name}"."{table}" SET {", ".join(set_clauses)};'
        logger.debug(f"Executing bulk data cleaning for table '{table}'...")
        conn.execute(text(clean_sql))

def _apply_constraints_for_table(conn, schema_name, table, rules):
    """Applies all constraints for a single table, one by one."""
    full_table_name = f'"{schema_name}"."{table}"'
    
    for rule in rules:
        column_name_raw = str(rule['column_name']).strip()
        constraint_type = str(rule['constraint_type']).upper()
        value = rule.get('value')
        alter_sql = ""

        if constraint_type == "MAX":
            constraint_name = f"CK_{table}_{column_name_raw}_max"
            alter_sql = f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" CHECK ("{column_name_raw}" <= {value})'
        elif constraint_type == "MIN":
            constraint_name = f"CK_{table}_{column_name_raw}_min"
            alter_sql = f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" CHECK ("{column_name_raw}" >= {value})'
        elif constraint_type == "UNIQUE":
            cols_for_unique = [c.strip() for c in column_name_raw.split(';')]
            cols_tuple_sql = ", ".join([f"'{c}'" for c in cols_for_unique])
            types_query = text(f"SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table}' AND COLUMN_NAME IN ({cols_tuple_sql})")
            col_types = conn.execute(types_query).fetchall()
            
            if any(max_len == -1 for _, max_len in col_types):
                logger.warning(f"Cannot create UNIQUE constraint on '{full_table_name}' for columns {cols_for_unique} because one has a MAX data type. Skipping.")
                continue

            unique_cols_sql = ", ".join([f'"{c}"' for c in cols_for_unique])
            constraint_name_cols = '_'.join(cols_for_unique)
            constraint_name = f"UQ_{table}_{constraint_name_cols}"
            alter_sql = f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" UNIQUE ({unique_cols_sql})'

        if alter_sql:
            try:
                logger.debug(f"Applying constraint '{constraint_name}' to {full_table_name}...")
                conn.execute(text(alter_sql))
            except SQLAlchemyError as e:
                logger.warning(f"Could not apply constraint '{constraint_name}' to {full_table_name}. Reason: {e.orig}")

def apply_data_quality_rules(target_db_url: str, schema_name: str, csv_path: str = "config/data_quality_changes/constraints.csv"):
    """
    Orchestrates the application of data quality rules from CSV and automatic date checks.
    """
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        # Part 1: Drop all existing CHECK and UNIQUE constraints for a clean slate.
        _drop_existing_constraints(conn, schema_name)
        
        # Part 2: Handle automatic date constraints
        # *** FIX: Removed the extra argument from the function call ***
        _apply_automatic_date_rules(conn, schema_name)

        # Part 3: Handle CSV-based constraints
        logger.info("Starting CSV-based constraint application...")
        constraints_df = load_schema_changes(csv_path)
        if constraints_df.empty:
            logger.info("No constraints found in CSV file. Skipping.")
            return
            
        grouped_constraints = constraints_df.groupby('table_name')
        
        for table, group_df in grouped_constraints:
            logger.info(f"Processing CSV constraints for table: {table}")
            try:
                # Clean all data for this table based on all its rules
                _clean_data_for_table(conn, schema_name, table, group_df.to_dict('records'))
                # Apply all constraints for this table
                _apply_constraints_for_table(conn, schema_name, table, group_df.to_dict('records'))
            except SQLAlchemyError as e:
                logger.warning(f"A failure occurred while processing table {table}. All changes for this table were rolled back. Error: {e}")
                continue
