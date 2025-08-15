from collections import defaultdict
from sqlalchemy.exc import DBAPIError

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from utils.logger import setup_logger

logger = setup_logger()


# --- Sub-functions ---
def _update_rebaseline_column_in_place(conn, schema_name: str):
    """
    Updates the 'is_subject_to_rebaseline' column values directly.
    ...
    """
    table_name = 'project_summary'
    column_name = 'is_subject_to_rebaseline'

    logger.debug(f"Updating data in-place for column '{column_name}'...")

    try:
        update_sql = text(f'''
            UPDATE "{schema_name}"."{table_name}"
            SET
                "{column_name}" = CASE
                    WHEN UPPER(TRIM(CAST("{column_name}" AS NVARCHAR(MAX)))) = 'SUBJECT TO REBASELINE' THEN '1'
                    ELSE '0'
                END;
        ''')
        result = conn.execute(update_sql)
        logger.debug(f"In-place update complete. {result.rowcount} rows affected.")
    except Exception as e:
        logger.error(f"Execution failed for _update_rebaseline_column_in_place: {e}")
        raise # Re-raise the exception to trigger the outer rollback

def _convert_varchar_to_nvarchar(conn, schema_name: str):
    """
    Finds all VARCHAR columns in a given schema and converts them to NVARCHAR,
    preserving their original length.

    This function is designed to run within a transaction. It will attempt to
    convert all eligible columns and will log any individual failures. If one or
    more conversions fail, it raises a RuntimeError at the end to signal
    that the parent transaction should be rolled back.

    Args:
        conn: An active SQLAlchemy connection object.
        schema_name (str): The name of the schema to scan for VARCHAR columns.
    
    Raises:
        RuntimeError: If one or more columns could not be converted.
    """
    logger.debug(f"Starting VARCHAR to NVARCHAR conversion for schema: '{schema_name}'")

    # Key Fix 1: Use LOWER() for case-insensitive matching and parameter binding for security.
    varchar_query = text("""
        SELECT c.TABLE_NAME, c.COLUMN_NAME, c.CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        JOIN INFORMATION_SCHEMA.TABLES AS t
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
        WHERE
            t.TABLE_TYPE = 'BASE TABLE'
            AND c.TABLE_SCHEMA = :schema_name
            AND LOWER(c.DATA_TYPE) = 'varchar';
    """)

    try:
        varchar_columns = conn.execute(varchar_query, {"schema_name": schema_name}).fetchall()
    except DBAPIError as e:
        logger.error(f"Failed to query for VARCHAR columns. Permissions issue? Error: {e.orig}")
        raise

    if not varchar_columns:
        logger.debug("No VARCHAR columns found in tables to convert. Task complete.")
        return

    logger.debug(f"Found {len(varchar_columns)} VARCHAR columns to convert.")
    converted_count = 0
    failed_count = 0

    for table, column, length in varchar_columns:
        # This logic correctly handles VARCHAR(MAX), which has a length of -1
        length_def = f"({length})" if length != -1 else "(MAX)"
        full_column_name = f"[{schema_name}].[{table}].[{column}]"

        # Key Fix 2: Use SQL Server-native quoting ([]) for identifiers.
        alter_sql = text(f'ALTER TABLE {full_column_name.rsplit(".", 1)[0]} ALTER COLUMN [{column}] NVARCHAR{length_def}')

        try:
            logger.debug(f"Attempting to convert {full_column_name} to NVARCHAR{length_def}...")
            conn.execute(alter_sql)
            logger.debug(f"Successfully converted {full_column_name}.")
            converted_count += 1
        # Key Fix 3: Catch errors on a per-column basis for robust execution.
        except DBAPIError as e:
            logger.error(f"FAILED to convert {full_column_name}. Reason: {e.orig}")
            failed_count += 1
        except Exception as e:
            logger.error(f"An unexpected error occurred while converting {full_column_name}: {e}")
            failed_count += 1

    logger.debug("--- Conversion Summary ---")
    logger.debug(f"Successfully converted: {converted_count} columns.")
    logger.warning(f"Failed to convert: {failed_count} columns. Check logs for details.")
    logger.debug("VARCHAR to NVARCHAR conversion process finished.")

    # Key Fix 4: Raise an error to trigger a transaction rollback if anything failed.
    if failed_count > 0:
        raise RuntimeError(f"{failed_count} columns could not be converted. Transaction will be rolled back.")


def _convert_text_to_nvarchar_max(conn, schema_name: str):
    """
    Finds all TEXT or NTEXT columns in a schema and converts them to NVARCHAR(MAX).
    This version correctly ignores views.
    """
    logger.debug("Phase 2: Converting TEXT/NTEXT columns to NVARCHAR(MAX)...")
    text_query = text(f"""
        SELECT c.TABLE_NAME, c.COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        JOIN INFORMATION_SCHEMA.TABLES AS t
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
        WHERE
            t.TABLE_TYPE = 'BASE TABLE'
            AND c.TABLE_SCHEMA = '{schema_name}'
            AND c.DATA_TYPE IN ('text', 'ntext');
    """)

    text_columns = conn.execute(text_query).fetchall()
    if not text_columns:
        logger.debug("No TEXT or NTEXT columns found in tables to convert.")
        return

    logger.debug(f"Found {len(text_columns)} TEXT/NTEXT columns to convert to NVARCHAR(MAX).")
    for table, column in text_columns:
        logger.debug(f"Converting {schema_name}.{table}.{column} to NVARCHAR(MAX)")
        alter_sql = text(f'ALTER TABLE "{schema_name}"."{table}" ALTER COLUMN "{column}" NVARCHAR(MAX)')
        conn.execute(alter_sql)
    logger.debug("Phase 2: TEXT to NVARCHAR(MAX) conversion complete.")

def _apply_changes_from_csv(conn, schema_name: str, csv_path: str):
    """
    Applies specific data type changes from a CSV file. This version robustly
    cleans and preserves numeric, date, and bit data while nullifying only truly non-convertible values.
    """
    try:
        dtypes_df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Could not load schema changes from {csv_path}. Aborting. Error: {e}")
        raise

    if dtypes_df.empty:
        logger.debug("No changes specified in CSV. Skipping this phase.")
        return

    where_clauses = []
    for _, row in dtypes_df.iterrows():
        table = str(row['table_name']).strip().replace("'", "''")
        column = str(row['column_name']).strip().replace("'", "''")
        where_clauses.append(f"(TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}')")

    if not where_clauses:
        return

    full_where_clause = " OR ".join(where_clauses)

    metadata_query = text(f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND ({full_where_clause});
    """)

    metadata_cache = {(row.TABLE_NAME, row.COLUMN_NAME): row for row in conn.execute(metadata_query).fetchall()}

    for _, row in dtypes_df.iterrows():
        table = str(row['table_name']).strip()
        column = str(row['column_name']).strip()
        new_type_from_csv = str(row['new_data_type']).strip()

        db_metadata = metadata_cache.get((table, column))
        if not db_metadata:
            logger.warning(f"Column {schema_name}.{table}.{column} from CSV does not exist. Skipping.")
            continue

        try:
            actual_db_type = db_metadata.DATA_TYPE.lower()
            string_db_types = ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext')

            if actual_db_type in string_db_types:
                target_base_type = new_type_from_csv.lower().split('(')[0]
                safe_table = f'"{schema_name}"."{table}"'
                safe_column = f'"{column}"'

                if target_base_type == 'bit':
                    logger.debug(f"Robustly cleaning {safe_table}.{safe_column} for BIT conversion.")
                    clean_and_update_sql = text(f"""
                        UPDATE {safe_table}
                        SET {safe_column} = CASE
                            WHEN UPPER(LTRIM(RTRIM({safe_column}))) IN ('1', 'TRUE', 'T', 'Y', 'YES') THEN '1'
                            WHEN UPPER(LTRIM(RTRIM({safe_column}))) IN ('0', 'FALSE', 'F', 'N', 'NO') THEN '0'
                            ELSE NULL
                        END;
                    """)
                    conn.execute(clean_and_update_sql)

                elif target_base_type in ('float', 'real', 'int', 'bigint', 'decimal', 'numeric', 'money'):
                    logger.debug(f"Robustly cleaning and preserving {safe_table}.{safe_column} for numeric conversion.")
                    robust_cleaned_column = f"TRIM(REPLACE(REPLACE({safe_column}, NCHAR(160), N' '), NCHAR(9), N' '))"
                    clean_and_update_sql = text(f"""
                        UPDATE {safe_table}
                        SET {safe_column} = CASE
                            WHEN TRY_CONVERT({new_type_from_csv}, {robust_cleaned_column}) IS NOT NULL THEN {robust_cleaned_column}
                            ELSE NULL
                        END;
                    """)
                    conn.execute(clean_and_update_sql)

                elif target_base_type in ('date', 'datetime', 'datetime2'):
                    logger.debug(f"Robustly cleaning {safe_table}.{safe_column} for date conversion.")
                    clean_and_update_sql = text(f"""
                        UPDATE {safe_table}
                        SET {safe_column} = CASE
                            WHEN LTRIM(RTRIM({safe_column})) IN ('', 'NULL', 'NA', 'N/A') THEN NULL
                            WHEN TRY_CONVERT({new_type_from_csv}, {safe_column}) IS NOT NULL THEN {safe_column}
                            ELSE NULL
                        END
                        WHERE {safe_column} IS NOT NULL;
                    """)
                    conn.execute(clean_and_update_sql)

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
            logger.debug("--- Phase 1: Applying some data cleaning... ---")
            _update_rebaseline_column_in_place(conn, schema_name)
            logger.debug("--- Phase 1: Applying some data cleaning complete. ---")

            logger.debug("--- Phase 2: Applying specific type changes from CSV file... ---")
            _apply_changes_from_csv(conn, schema_name, csv_path)
            logger.debug("--- Phase 2: CSV changes complete. ---")

            logger.debug("--- Phase 3: Converting TEXT/NTEXT columns to NVARCHAR(MAX)... ---")
            _convert_text_to_nvarchar_max(conn, schema_name)
            logger.debug("--- Phase 3: TEXT to NVARCHAR(MAX) conversion complete. ---")

            logger.debug("--- Phase 4: Converting VARCHAR columns to NVARCHAR... ---")
            _convert_varchar_to_nvarchar(conn, schema_name)
            logger.debug("--- Phase 4: VARCHAR to NVARCHAR conversion complete. ---")
            
        logger.debug("Data type conversion process completed successfully.")
    except Exception as e:
        logger.error(f"The data type conversion process failed. The transaction has been rolled back. Error: {e}")
        raise

def set_health_safety_default(target_db_url: str, schema_name: str):
    """
    Sets a default value of 1 for the health and safety lookup column.
    """
    table_name = 'project_summary'
    column_name = 'is_external_lookup_for_health_and_safety'
    constraint_name = f"DF_{table_name}_{column_name}"

    logger.debug(
        f"Attempting to set default value for column '{column_name}' "
        f"in table '{schema_name}.{table_name}'."
    )

    set_default_sql = text(f"""
        DECLARE @SchemaName NVARCHAR(128) = :schema_name;
        DECLARE @TableName NVARCHAR(128) = :table_name;
        DECLARE @ColumnName NVARCHAR(128) = :column_name;
        DECLARE @ExistingConstraintName NVARCHAR(255);
        DECLARE @DynamicSQL NVARCHAR(MAX);

        SELECT @ExistingConstraintName = dc.name
        FROM sys.default_constraints dc
        JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = @SchemaName
          AND t.name = @TableName
          AND c.name = @ColumnName;

        IF @ExistingConstraintName IS NOT NULL
        BEGIN
            SET @DynamicSQL = N'ALTER TABLE ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) +
                              N' DROP CONSTRAINT ' + QUOTENAME(@ExistingConstraintName);
            EXEC sp_executesql @DynamicSQL;
        END;

        ALTER TABLE {schema_name}.{table_name}
        ADD CONSTRAINT {constraint_name} DEFAULT 1 FOR {column_name};
    """)

    try:
        engine = create_engine(target_db_url)
        with engine.begin() as conn:
            conn.execute(
                set_default_sql,
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "column_name": column_name
                }
            )
        logger.debug(
            f"Successfully set default value for '{column_name}' to 1."
        )
    except Exception as e:
        logger.error(
            f"Failed to set default constraint on '{schema_name}.{table_name}': {e}"
        )
        raise

def _drop_existing_constraints(conn, schema_name):
    """
    Finds and drops all existing CHECK, UNIQUE constraints, and UNIQUE INDEXED VIEWS.
    """
    logger.debug(f"Dropping all existing constraints and indexed views in schema '{schema_name}'...")
    
    # --- Part 1: Drop standard constraints and hash columns (your existing logic) ---
    constraints_to_drop_query = text("""...""") # Your existing query here
    # ... your existing try/except block for dropping table constraints ...

    # --- Part 2: Drop unique indexed views ---
    indexed_views_query = text("""
        SELECT s.name AS schema_name, v.name AS view_name
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE EXISTS (
            SELECT 1 FROM sys.indexes i
            WHERE i.object_id = v.object_id
              AND i.is_unique = 1 AND i.type_desc = 'CLUSTERED'
        ) AND s.name = :schema;
    """)
    try:
        views_to_drop = conn.execute(indexed_views_query, {"schema": schema_name}).fetchall()
        if views_to_drop:
            logger.debug(f"Found {len(views_to_drop)} unique indexed views to drop.")
            for s_name, v_name in views_to_drop:
                drop_view_sql = text(f'DROP VIEW "{s_name}"."{v_name}";')
                conn.execute(drop_view_sql)
                logger.debug(f"Dropped indexed view: '{s_name}.{v_name}'.")
    except Exception as e:
        logger.error(f"Failed during indexed view dropping phase: {e}")
        raise

def _apply_automatic_date_rules(conn, schema_name):
    """
    Finds all date/datetime columns and applies a MIN YEAR 2000 CHECK constraint.
    """
    logger.debug("Applying automatic date constraints (MIN YEAR 2000)...")
    date_cols_query = text("""
        SELECT c.TABLE_NAME, c.COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS c
        JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND c.TABLE_SCHEMA = :schema
          AND c.DATA_TYPE IN ('date', 'datetime', 'datetime2', 'smalldatetime')
          AND c.COLUMN_NAME NOT IN ('last_updated');
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
    """
    Applies all constraints for a single table. It is now robust against non-deterministic
    data types, single-column unique constraints, and non-existent columns in rules.
    Operations are wrapped in a nested transaction (SAVEPOINT).
    """
    full_table_name = f'"{schema_name}"."{table}"'
    
    transaction = conn.begin_nested()
    try:
        for rule in rules:
            column_name_raw = str(rule['column_name']).strip()
            constraint_type = str(rule['constraint_type']).upper()
            
            if constraint_type in ("MAX", "MIN"):
                value = rule.get('value')
                column_name = column_name_raw.split(';')[0].strip()
                op = "<=" if constraint_type == "MAX" else ">="
                constraint_name = f"CK_{table}_{column_name}_{constraint_type.lower()}"
                
                check_sql = text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" CHECK ("{column_name}" {op} {value})')
                
                try:
                    logger.debug(f"Applying CHECK constraint '{constraint_name}'...")
                    conn.execute(check_sql)
                except SQLAlchemyError as e:
                    logger.warning(f"Could not apply CHECK constraint '{constraint_name}'. Reason: {e.orig}")
                continue

            elif constraint_type == "UNIQUE":
                cols_for_unique = [c.strip() for c in column_name_raw.split(';')]
                
                # --- FIX 3: Validate that all specified columns exist in the table ---
                cols_placeholder = ", ".join([f"'{c}'" for c in cols_for_unique])
                validation_query = text(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table}'
                    AND COLUMN_NAME IN ({cols_placeholder})
                """)
                if conn.execute(validation_query).scalar() != len(cols_for_unique):
                    logger.error(f"Rule skipped: Not all columns for UNIQUE constraint ({', '.join(cols_for_unique)}) exist in table {full_table_name}.")
                    continue # Skip this rule and move to the next

                # --- Build the sanitization and concatenation SQL ---
                sanitized_cols_sql = []
                for col in cols_for_unique:
                    # --- FIX 1: Make hash deterministic ---
                    # Use ISNULL and CONVERT instead of CAST to ensure deterministic string conversion.
                    formula = (
                        "UPPER(TRIM(REPLACE(REPLACE(REPLACE("
                        f"ISNULL(CONVERT(NVARCHAR(4000), \"{col}\", 121), ''), " # Style 121 is ODBC canonical, good for determinism
                        "CHAR(13), ' '), CHAR(10), ' '), CHAR(160), ' ')))"
                    )
                    sanitized_cols_sql.append(formula)
                
                # --- FIX 2: Handle single-column UNIQUE constraints ---
                if len(sanitized_cols_sql) > 1:
                    concat_sql = f"CONCAT_WS('||', {', '.join(sanitized_cols_sql)})"
                else:
                    # If only one column, no need for CONCAT_WS
                    concat_sql = sanitized_cols_sql[0]

                # --- Define names ---
                constraint_name_cols = '_'.join(cols_for_unique)
                hash_column_name = f"uq_hash_{constraint_name_cols}"
                constraint_name = f"UQ_{table}_{constraint_name_cols}"

                # --- Build ADD COLUMN and ADD CONSTRAINT statements ---
                add_col_sql = text(f"""
                ALTER TABLE {full_table_name}
                ADD "{hash_column_name}" AS (
                    HASHBYTES('SHA2_256', {concat_sql})
                ) PERSISTED
                """)

                add_constraint_sql = text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" UNIQUE ("{hash_column_name}")')

                # --- Execute statements ---
                try:
                    logger.debug(f"Creating hash column '{hash_column_name}' on {full_table_name}...")
                    conn.execute(add_col_sql)
                except SQLAlchemyError as e:
                    if "already exists" in str(e.orig):
                        logger.debug(f"Hash column '{hash_column_name}' already exists. Skipping creation.")
                    else:
                        logger.error(f"Failed to create hash column '{hash_column_name}'. Reason: {e.orig}")
                        raise

                try:
                    logger.debug(f"Applying UNIQUE constraint '{constraint_name}'...")
                    conn.execute(add_constraint_sql)
                except SQLAlchemyError as e:
                    logger.warning(f"Could not apply UNIQUE constraint '{constraint_name}'. Reason: {e.orig}")
                    raise

        # If the loop completes, commit the nested transaction
        transaction.commit()
        logger.debug(f"Successfully processed constraints for table {full_table_name}.")

    except Exception as e:
        # If any error occurs, roll back the nested transaction
        logger.error(f"An unexpected error occurred while processing {full_table_name}. Rolling back changes for this table.")
        transaction.rollback()
        raise

def _apply_indexed_view_rules(conn, schema_name, view_rules_df):
    """
    Processes rules of type 'UNIQUE_VIEW' from the dataframe to create unique indexed views.
    This version uses the correct SQL Server syntax for naming columns in a schema-bound view.
    """
    if view_rules_df.empty:
        return
    logger.debug("Applying UNIQUE_VIEW (indexed view) constraints...")
    for _, rule in view_rules_df.iterrows():
        try:
            view_name = rule['table_name']
            columns_str = rule['column_name']
            join_tables_str = rule['join_tables']
            join_condition = rule['join_condition']

            # --- Parse CSV inputs ---
            # 'ps.period;ps.project_name' -> ['period', 'project_name']
            select_cols_unquoted = [c.split(".")[-1].strip() for c in columns_str.split(';')]
            # 'ps.period;ps.project_name' -> '"period", "project_name"'
            index_cols_quoted = [f'"{col}"' for col in select_cols_unquoted]
            
            select_clause = ', '.join(c.strip() for c in columns_str.split(';'))
            
            table_definitions = join_tables_str.split(';')
            table_joins_list = []
            for t_def in table_definitions:
                parts = [p.strip() for p in t_def.split(' AS ')]
                table_name, alias = parts[0], parts[1]
                sql_fragment = f'"{schema_name}"."{table_name}" AS {alias}'
                table_joins_list.append(sql_fragment)
            from_clause_schemabound = ' INNER JOIN '.join(table_joins_list)

            # --- Define Names and Final SQL ---
            full_view_name = f'"{schema_name}"."{view_name}"'
            index_name = f'UX_{view_name}'
            
            # --- FIX: Define all column names for the view in the CREATE VIEW line ---
            # This includes a name for the COUNT_BIG(*) column, e.g., "RecordCount"
            view_column_names = index_cols_quoted + ['"RecordCount"']
            view_def_columns = f"({', '.join(view_column_names)})"

            create_view_sql = text(f"""
                CREATE VIEW {full_view_name} {view_def_columns}
                WITH SCHEMABINDING
                AS
                SELECT
                    {select_clause},
                    COUNT_BIG(*)
                FROM
                    {from_clause_schemabound}
                ON
                    {join_condition}
                GROUP BY
                    {select_clause};
            """)

            create_index_sql = text(f"""
                CREATE UNIQUE CLUSTERED INDEX {index_name}
                ON {full_view_name} ({', '.join(index_cols_quoted)});
            """)

            logger.debug(f"Creating schema-bound view: {view_name}")
            conn.execute(create_view_sql)
            
            logger.debug(f"Creating unique clustered index on view: {index_name}")
            conn.execute(create_index_sql)
            logger.debug(f"SUCCESS: Created indexed view {view_name} to enforce uniqueness.")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create indexed view for rule '{rule['table_name']}'. Reason: {e.orig}")
            raise # Re-raise to ensure transaction rollback

def apply_data_quality_rules(target_db_url: str, schema_name: str, csv_path: str = "config/data_quality_changes/constraints.csv"):
    """
    Orchestrates the application of data quality rules from CSV and automatic date checks.
    """
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        _drop_existing_constraints(conn, schema_name)
        _apply_automatic_date_rules(conn, schema_name)

        logger.debug("Starting CSV-based constraint application...")
        try:
            constraints_df = pd.read_csv(csv_path)
            # Fill NaN for new columns to avoid errors
            constraints_df.fillna({'join_tables': '', 'join_condition': ''}, inplace=True)
        except FileNotFoundError:
            logger.warning(f"Constraint CSV file not found at {csv_path}. Skipping CSV rules.")
            return
            
        if constraints_df.empty:
            logger.debug("No constraints found in CSV file. Skipping.")
            return
        
        # --- MODIFICATION: Separate view rules from table rules ---
        view_rules_df = constraints_df[constraints_df['constraint_type'] == 'UNIQUE_VIEW'].copy()
        table_rules_df = constraints_df[constraints_df['constraint_type'] != 'UNIQUE_VIEW'].copy()

        # --- Call the new function for view-based rules ---
        _apply_indexed_view_rules(conn, schema_name, view_rules_df)

        # --- Continue with existing logic for table-based rules ---
        grouped_constraints = table_rules_df.groupby('table_name')
        
        for table, group_df in grouped_constraints:
            logger.debug(f"Processing table-based CSV constraints for table: {table}")
            try:
                _clean_data_for_table(conn, schema_name, table, group_df.to_dict('records'))
                # Pass the original _apply_constraints_for_table function
                _apply_constraints_for_table(conn, schema_name, table, group_df.to_dict('records'))
            except SQLAlchemyError as e:
                logger.warning(f"A failure occurred while processing table '{table}'. All changes for this table were rolled back. Error: {e.orig}")
                continue