import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

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
