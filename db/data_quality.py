from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from utils.logger import setup_logger
from utils.utils import clean_column_for_datetime_conversion

logger = setup_logger()


# --- Sub-functions ---

def _convert_varchar_to_nvarchar(conn, schema_name: str):
    """
    Finds all VARCHAR columns in a schema and converts them to NVARCHAR,
    preserving their original length.
    """
    varchar_query = text(f"""
        SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND DATA_TYPE = 'varchar';
    """)
    
    varchar_columns = conn.execute(varchar_query).fetchall()
    if not varchar_columns:
        logger.debug("No VARCHAR columns found to convert.")
        return

    logger.debug(f"Found {len(varchar_columns)} VARCHAR columns to convert to NVARCHAR.")
    for table, column, length in varchar_columns:
        length_def = f"({length})" if length != -1 else "(MAX)"
        logger.debug(f"Converting {schema_name}.{table}.{column} to NVARCHAR{length_def}")
        alter_sql = text(f'ALTER TABLE "{schema_name}"."{table}" ALTER COLUMN "{column}" NVARCHAR{length_def}')
        conn.execute(alter_sql)

def _convert_text_to_nvarchar_max(conn, schema_name: str):
    """
    Finds all TEXT or NTEXT columns in a schema and converts them to NVARCHAR(MAX).
    """
    text_query = text(f"""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND DATA_TYPE IN ('text', 'ntext');
    """)
    
    text_columns = conn.execute(text_query).fetchall()
    if not text_columns:
        logger.debug("No TEXT or NTEXT columns found to convert.")
        return

    logger.debug(f"Found {len(text_columns)} TEXT/NTEXT columns to convert to NVARCHAR(MAX).")
    for table, column in text_columns:
        logger.debug(f"Converting {schema_name}.{table}.{column} to NVARCHAR(MAX)")
        alter_sql = text(f'ALTER TABLE "{schema_name}"."{table}" ALTER COLUMN "{column}" NVARCHAR(MAX)')
        conn.execute(alter_sql)

def _apply_changes_from_csv(engine, conn, schema_name: str, csv_path: str):
    """
    Applies specific data type changes from a CSV file, including the
    original data cleaning logic, using SQL Server-compatible syntax.
    """
    try:
        # This function should be defined in your project to load the CSV
        dtypes_df = pd.read_csv(csv_path) 
    except Exception as e:
        logger.error(f"Could not load schema changes from {csv_path}. Aborting. Error: {e}")
        raise

    if dtypes_df.empty:
        logger.debug("No changes specified in CSV. Skipping this phase.")
        return

    # --- THE FIX: Build a WHERE clause compatible with SQL Server ---
    where_clauses = []
    for _, row in dtypes_df.iterrows():
        # Sanitize table and column names to prevent issues
        table = str(row['table_name']).strip().replace("'", "''")
        column = str(row['column_name']).strip().replace("'", "''")
        where_clauses.append(f"(TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}')")

    if not where_clauses:
        return

    full_where_clause = " OR ".join(where_clauses)
    
    metadata_query = text(f"""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND ({full_where_clause});
    """)
    # -----------------------------------------------------------------
    
    metadata_cache = {(row.TABLE_NAME, row.COLUMN_NAME): row for row in conn.execute(metadata_query).fetchall()}

    for _, row in dtypes_df.iterrows():
        table = str(row['table_name']).strip()
        column = str(row['column_name']).strip()
        old_type_from_csv = str(row['old_data_type']).strip()
        new_type_from_csv = str(row['new_data_type']).strip()

        if (table, column) not in metadata_cache:
            logger.warning(f"Column {schema_name}.{table}.{column} from CSV does not exist. Skipping.")
            continue

        try:
            # --- Restored Original Data Cleaning Logic ---
            string_like_old_types = ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext')
            is_old_type_string_like = any(old_type_from_csv.lower().startswith(s_type) for s_type in string_like_old_types)

            if is_old_type_string_like:
                target_base_type = new_type_from_csv.lower().split('(')[0]
                safe_table = f'"{schema_name}"."{table}"'
                safe_column = f'"{column}"'

                if target_base_type == 'bit':
                    logger.debug(f"Cleaning {safe_table}.{safe_column} for BIT conversion.")
                    clean_sql = text(f"UPDATE {safe_table} SET {safe_column} = NULL WHERE LTRIM(RTRIM(CAST({safe_column} AS VARCHAR(MAX)))) = '' OR (CAST({safe_column} AS VARCHAR(MAX)) NOT IN ('0', '1') AND {safe_column} IS NOT NULL);")
                    conn.execute(clean_sql)
                
                elif target_base_type in ('float', 'real', 'int', 'bigint', 'decimal', 'numeric', 'money'):
                    logger.debug(f"Cleaning {safe_table}.{safe_column} for numeric conversion.")
                    clean_sql = text(f"UPDATE {safe_table} SET {safe_column} = NULL WHERE TRY_CONVERT({new_type_from_csv}, {safe_column}) IS NULL AND {safe_column} IS NOT NULL;")
                    conn.execute(clean_sql)

                elif target_base_type in ('date', 'datetime', 'datetime2'):
                    logger.debug(f"Cleaning {safe_table}.{safe_column} for datetime conversion.")
                    clean_column_for_datetime_conversion(conn, schema_name, table, column)
                    final_clean_sql = text(f"UPDATE {safe_table} SET {safe_column} = NULL WHERE {safe_column} IS NOT NULL AND TRY_CONVERT({new_type_from_csv}, {safe_column}) IS NULL;")
                    conn.execute(final_clean_sql)

            logger.debug(f"Attempting to alter {schema_name}.{table}.{column} to type {new_type_from_csv}.")
            alter_sql = text(f'ALTER TABLE "{schema_name}"."{table}" ALTER COLUMN "{column}" {new_type_from_csv}')
            conn.execute(alter_sql)
            logger.debug(f"Successfully changed type of {schema_name}.{table}.{column} to {new_type_from_csv}.")

        except Exception as e:
            logger.error(f"Failed to process CSV change for column {schema_name}.{table}.{column}. Error: {e}")
            raise

# --- Main Controller Function ---

def change_data_types(target_db_url: str, schema_name: str, csv_path: str = "config/data_quality_changes/data_types.csv"):
    """
    Main controller function to run all data type conversion tasks in the correct order.
    """
    logger.debug(f"Starting data type conversion process for schema '{schema_name}'...")
    engine = create_engine(target_db_url)
    
    try:
        with engine.begin() as conn: # All changes are in one transaction
            
            logger.debug("--- Phase 1: Applying specific type changes from CSV file... ---")
            _apply_changes_from_csv(engine, conn, schema_name, csv_path)
            logger.debug("--- Phase 1: CSV changes complete. ---")

            logger.debug("--- Phase 2: Converting TEXT/NTEXT columns to NVARCHAR(MAX)... ---")
            _convert_text_to_nvarchar_max(conn, schema_name)
            logger.debug("--- Phase 2: TEXT to NVARCHAR(MAX) conversion complete. ---")

            logger.debug("--- Phase 3: Converting VARCHAR columns to NVARCHAR... ---")
            _convert_varchar_to_nvarchar(conn, schema_name)
            logger.debug("--- Phase 3: VARCHAR to NVARCHAR conversion complete. ---")
            
        logger.debug("Data type conversion process completed successfully.")
    except Exception as e:
        logger.error(f"The data type conversion process failed. The transaction has been rolled back. Error: {e}")
        raise

def _drop_existing_constraints(conn, schema_name):
    """
    Finds and drops all existing CHECK and UNIQUE constraints in the schema.
    This ensures a clean slate before applying new rules.
    """
    logger.debug(f"Dropping all existing CHECK and UNIQUE constraints in schema '{schema_name}'...")
    
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
            logger.debug("No existing CHECK or UNIQUE constraints to drop.")
            return

        logger.debug(f"Found {len(constraints)} constraints to drop.")
        for table, constraint_name in constraints:
            try:
                drop_sql = f'ALTER TABLE "{schema_name}"."{table}" DROP CONSTRAINT "{constraint_name}";'
                conn.execute(text(drop_sql))
                logger.debug(f"Dropped constraint '{constraint_name}' from table '{table}'.")
            except SQLAlchemyError as e:
                logger.warning(f"Could not drop constraint '{constraint_name}' from '{table}'. It might be in use. Error: {e.orig}")
        logger.debug("Finished dropping existing constraints.")
    except Exception as e:
        logger.error(f"Failed during the constraint dropping phase: {e}")
        raise

def _apply_automatic_date_rules(conn, schema_name):
    """
    Finds all date/datetime columns and applies a MIN YEAR 2000 CHECK constraint.
    """
    logger.debug("Applying automatic date constraints (MIN YEAR 2000)...")
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
        logger.debug("Starting CSV-based constraint application...")
        constraints_df = pd.read_csv(csv_path)
        if constraints_df.empty:
            logger.debug("No constraints found in CSV file. Skipping.")
            return
            
        grouped_constraints = constraints_df.groupby('table_name')
        
        for table, group_df in grouped_constraints:
            logger.debug(f"Processing CSV constraints for table: {table}")
            try:
                # Clean all data for this table based on all its rules
                _clean_data_for_table(conn, schema_name, table, group_df.to_dict('records'))
                # Apply all constraints for this table
                _apply_constraints_for_table(conn, schema_name, table, group_df.to_dict('records'))
            except SQLAlchemyError as e:
                logger.warning(f"A failure occurred while processing table {table}. All changes for this table were rolled back. Error: {e}")
                continue
