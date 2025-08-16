import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from config.constants import PROCESSING_CHUNK_SIZE
from utils.logger import setup_logger

logger = setup_logger()

def add_primary_keys(target_db_url, schema_name):
    """
    For MSSQL, assertively ensures 'id' is the primary key.
    It also checks if the 'id' column has the IDENTITY property and logs a
    warning for tables that require manual intervention, as this cannot be
    safely automated in SQL Server.
    """
    engine = create_engine(target_db_url)
    dialect_name = engine.dialect.name

    if dialect_name != 'mssql':
        logger.error(f"This script is specifically tuned for MSSQL. Detected dialect: {dialect_name}.")
        return

    excluded_tables = ('sessions')
    excluded_tables_sql_str = ", ".join([f"'{table}'" for table in excluded_tables])

    discovery_sql = text(f"""
        SELECT
            t.TABLE_NAME,
            c.COLUMN_NAME AS IdColumnName,
            pk.ConstraintName,
            pk.PkColumnName,
            COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IsIdentity
        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        LEFT JOIN (
            SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME AS ConstraintName, kcu.COLUMN_NAME AS PkColumnName
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.TABLE_SCHEMA = :schema_name AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) AS pk ON t.TABLE_SCHEMA = pk.TABLE_SCHEMA AND t.TABLE_NAME = pk.TABLE_NAME
        WHERE t.TABLE_SCHEMA = :schema_name
          AND t.TABLE_TYPE = 'BASE TABLE'
          AND LOWER(c.COLUMN_NAME) = 'id'
          AND t.TABLE_NAME NOT IN ({excluded_tables_sql_str});
    """)
    try:
        with engine.connect() as conn:
            results = conn.execute(discovery_sql, {"schema_name": schema_name}).fetchall()
            tables_to_process = [(row[0], row[1], row[2], row[3], row[4]) for row in results]
    except SQLAlchemyError:
        logger.exception(f"Fatal: Failed to query schema information.")
        return

    if not tables_to_process:
        logger.debug(f"No tables requiring changes were found in schema '{schema_name}'.")
        return

    logger.debug(f"Found {len(tables_to_process)} tables to process for PK and IDENTITY checks.")
    with engine.begin() as conn:
        for table_name, id_column_name, pk_constraint_name, pk_column_name, is_identity in tables_to_process:
            # For MSSQL, schema and table names need to be in brackets for safety
            full_table_name = f'[{schema_name}].[{table_name}]'
            is_pk_correct = pk_constraint_name and pk_column_name.lower() == id_column_name.lower()
            # is_identity returns 1 if it's an identity column, 0 if not.
            is_identity_correct = is_identity == 1

            if is_pk_correct and is_identity_correct:
                logger.debug(f"OK: {full_table_name} is already configured correctly.")
                continue

            # Log a clear warning for the missing IDENTITY property
            if not is_identity_correct:
                logger.warning(f"MANUAL FIX REQUIRED: {full_table_name} column '{id_column_name}' is not an IDENTITY column. This cannot be fixed automatically.")

            try:
                # Fix the Primary Key, which *can* be automated
                if not is_pk_correct:
                    if pk_constraint_name:
                        logger.debug(f"FIXING: {full_table_name} has a PK on the wrong column. Dropping '{pk_constraint_name}'.")
                        conn.execute(text(f'ALTER TABLE {full_table_name} DROP CONSTRAINT "{pk_constraint_name}";'))

                    logger.debug(f"ACTION: Adding primary key to {full_table_name} on column '{id_column_name}'...")
                    conn.execute(text(f'ALTER TABLE {full_table_name} ALTER COLUMN "{id_column_name}" INT NOT NULL;'))
                    conn.execute(text(f'ALTER TABLE {full_table_name} ADD PRIMARY KEY ("{id_column_name}");'))
                    logger.debug(f"SUCCESS: Primary key for {full_table_name} is now set.")

            except SQLAlchemyError:
                logger.exception(f"FAILED to set primary key for {full_table_name}. Skipping this table.")

def correct_and_update_design_managers(
    target_db_url: str,
    schema_name: str,
    primary_key_column: str = "id"
):
    """
    Connects to a database to clean the 'design_manager' column in the 'project_status' table.
    
    This function performs the following actions:
    1. Replaces all forward slashes ('/') with semicolons (';').
    2. Reads a master list of correct names from 'config/master_table_values/design_manager_correct_names.csv'.
    3. For each name in the 'design_manager' column, it checks against the 'wrong_name' column in the CSV.
    4. If a match is found (case-insensitive), it replaces it with the corresponding 'correct_name'.
    5. Updates the database rows only where changes have been made.

    Args:
        target_db_url: The database connection URL (e.g., "postgresql://user:password@host/dbname").
        schema_name: The name of the schema containing the 'project_status' table.
        primary_key_column: The name of the primary key column in 'project_status' (e.g., "id", "project_id").
                           This is essential for correctly identifying rows to update.
    """
    # --- 1. Load the Name Correction Map from CSV ---
    logger.info('Running necessary fixes on project_status.design_manager')
    try:
        csv_path = Path("config/master_table_values/design_manager_correct_names.csv")
        logger.debug(f"Loading name correction map from: {csv_path}")
        df_correct_names = pd.read_csv(csv_path)

        # Create a dictionary for fast lookups: { 'lowercase_wrong_name': 'Correct Name' }
        # We strip whitespace to ensure clean matching.
        correction_map = {
            str(row['wrong_name']).strip().lower(): str(row['correct_name']).strip()
            for _, row in df_correct_names.iterrows()
        }
        logger.debug(f"Successfully loaded {len(correction_map)} name corrections.")

    except FileNotFoundError:
        logger.error(f"Critical Error: The correction file was not found at {csv_path}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while reading the CSV file: {e}")
        raise

    try:
        # --- 2. Establish database connection and fetch data ---
        engine = create_engine(target_db_url)
        
        updates_to_perform = []

        with engine.begin() as conn:
            # Fetch all rows from the table to process them in Python
            select_query = text(f"""
                SELECT "{primary_key_column}", "design_manager"
                FROM "{schema_name}"."project_status"
                WHERE "design_manager" IS NOT NULL AND "design_manager" != '';
            """)
            logger.debug(f"Fetching data from \"{schema_name}\".\"project_status\"...")
            result = conn.execute(select_query)
            rows_to_process = result.fetchall()
            logger.debug(f"Found {len(rows_to_process)} rows to process.")

            # --- 3. Process each row to find necessary corrections ---
            for row in rows_to_process:
                pk_value, original_dm_string = row
                
                # First, standardize separators
                dm_with_semicolons = original_dm_string.replace('/', ';')

                # Split into individual names, stripping whitespace from each
                original_names = [name.strip() for name in dm_with_semicolons.split(';') if name.strip()]

                # Correct each name using the map
                corrected_names = []
                for name in original_names:
                    # Look up the lowercased, stripped name in our map
                    # If not found, use the original name as the default
                    corrected_name = correction_map.get(name.lower(), name)
                    corrected_names.append(corrected_name)
                
                # Re-join the names to form the final, corrected string
                final_dm_string = ";".join(corrected_names)
                
                # If the final string is different, stage it for update
                if final_dm_string != original_dm_string:
                    updates_to_perform.append({
                        'pk_param': pk_value,
                        'new_dm_value': final_dm_string
                    })

            # --- 4. Execute a single bulk update for all changed rows ---
            if not updates_to_perform:
                logger.debug("No design manager names required correction. Database is already up to date.")
                return

            logger.debug(f"Found {len(updates_to_perform)} rows that need updating. Executing bulk update...")
            
            update_query = text(f"""
                UPDATE "{schema_name}"."project_status"
                SET "design_manager" = :new_dm_value
                WHERE "{primary_key_column}" = :pk_param;
            """)
            
            # SQLAlchemy can execute the same statement with a list of parameter sets
            conn.execute(update_query, updates_to_perform)
            
            logger.debug(f"Operation complete. {len(updates_to_perform)} row(s) were successfully updated.")

    except Exception as e:
        logger.error(f"An error occurred during the database operation: {e}")
        raise

# Define constants if not globally available
PROCESSING_CHUNK_SIZE = 10000

def _parse_multi_value(value, separator: str):
    """
    Parses a string that can either be a simple separated list or a JSON array string.
    """
    if pd.isna(value):
        return None
    s_value = str(value).strip()
    if s_value.startswith('[') and s_value.endswith(']'):
        try:
            return json.loads(s_value)
        except json.JSONDecodeError:
            return []
    return [item.strip() for item in s_value.split(separator) if item.strip()]

def implement_many_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/many_to_many_relations.csv"):
    """
    Creates many-to-many relations with case-insensitive and whitespace-insensitive joins.
    """
    correct_and_update_design_managers(target_db_url, schema_name)
    MULTI_COLUMN_SEPARATOR = '-'
    try:
        relations_df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Could not load relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)

    # --- HELPER FOR CLEANING PANDAS STRINGS ---
    def pd_clean_string(series: pd.Series) -> pd.Series:
        """Replaces all whitespace types with a single space, then strips and lowercases."""
        # The regex r'[\s\xa0]+' matches one or more of any whitespace char OR a non-breaking space.
        return series.astype(str).str.replace(r'[\s\xa0]+', ' ', regex=True).str.strip().str.lower()

    for _, row in relations_df.iterrows():
        source_table, source_id_col = row["source_table"], row["source_id_column"]
        lookup_table, lookup_id_col = row["lookup_table"], row["lookup_id_column"]
        assoc_table, assoc_source_col, assoc_lookup_col = row["associative_table"], row["assoc_source_column"], row["assoc_lookup_column"]
        separator = row["seperator"]
        source_multi_cols = [c.strip() for c in row["source_multi_column"].split(MULTI_COLUMN_SEPARATOR)]
        lookup_name_cols = [c.strip() for c in row["lookup_name_column"].split(MULTI_COLUMN_SEPARATOR)]

        if len(source_multi_cols) != len(lookup_name_cols):
            logger.error(f"Mismatched number of lookup columns for associative table '{assoc_table}'. Skipping.")
            continue

        full_source_name = f'"{schema_name}"."{source_table}"'
        full_lookup_name = f'"{schema_name}"."{lookup_table}"'
        full_assoc_name = f'"{schema_name}"."{assoc_table}"'

        try:
            logger.debug(f"Processing relation for {full_assoc_name} with composite keys...")

            # --- Caching lookup table ---
            logger.debug(f"Caching lookup table: {full_lookup_name}...")
            quoted_lookup_cols = [f'"{c}"' for c in lookup_name_cols]
            lookup_sql = f'SELECT "{lookup_id_col}", {", ".join(quoted_lookup_cols)} FROM {full_lookup_name}'
            lookup_df = pd.read_sql(lookup_sql, engine)

            # UPDATED: Clean lookup keys to be lowercase and stripped of all whitespace types
            for col in lookup_name_cols:
                lookup_df[col] = pd_clean_string(lookup_df[col])

            multi_index = pd.MultiIndex.from_frame(lookup_df[lookup_name_cols])
            lookup_dict = pd.Series(lookup_df[lookup_id_col].values, index=multi_index).to_dict()
            logger.debug(f"Cached {len(lookup_dict)} lookup values using {len(lookup_name_cols)}-part composite key.")

            # --- Recreate target table ---
            with engine.begin() as conn:
                logger.debug(f"Recreating empty target table: {full_assoc_name}")
                conn.execute(text(f'DROP TABLE IF EXISTS {full_assoc_name};'))
                create_sql = f'CREATE TABLE {full_assoc_name} ("{assoc_source_col}" INT, "{assoc_lookup_col}" INT);'
                conn.execute(text(create_sql))

            # --- Streaming and Processing Source Table ---
            quoted_source_multi_cols = [f'"{c}"' for c in source_multi_cols]
            cols_to_select = [f'"{source_id_col}"'] + quoted_source_multi_cols
            where_clause = " AND ".join([f'{c} IS NOT NULL' for c in quoted_source_multi_cols])
            source_sql = f'SELECT {", ".join(cols_to_select)} FROM {full_source_name} WHERE {where_clause}'

            logger.debug(f"Streaming source table {full_source_name} in chunks of {PROCESSING_CHUNK_SIZE}...")
            total_unmatched_count = 0
            unmatched_samples = set()
            MAX_SAMPLES_TO_LOG = 10

            column_to_explode = source_multi_cols[-1]
            linking_columns = source_multi_cols[:-1]
            logger.debug(f"Will explode column '{column_to_explode}' and link with {linking_columns}.")

            for source_chunk_df in pd.read_sql(source_sql, engine, chunksize=PROCESSING_CHUNK_SIZE):
                source_chunk_df[column_to_explode] = source_chunk_df[column_to_explode].apply(_parse_multi_value, separator=separator)
                source_chunk_df.dropna(subset=[column_to_explode], inplace=True)
                exploded_df = source_chunk_df.explode(column_to_explode)
                exploded_df.dropna(subset=source_multi_cols, inplace=True)

                if exploded_df.empty:
                    continue

                # UPDATED: Clean source keys to be lowercase and stripped of all whitespace types
                for col in source_multi_cols:
                    exploded_df[col] = pd_clean_string(exploded_df[col])

                empty_string_mask = pd.Series(False, index=exploded_df.index)
                for col in source_multi_cols:
                    empty_string_mask |= (exploded_df[col] == '')
                exploded_df = exploded_df[~empty_string_mask]

                if exploded_df.empty:
                    continue

                multi_index = pd.MultiIndex.from_frame(exploded_df[source_multi_cols])
                exploded_df[assoc_lookup_col] = multi_index.map(lookup_dict)

                unmatched_in_chunk_df = exploded_df[exploded_df[assoc_lookup_col].isna()]
                total_unmatched_count += len(unmatched_in_chunk_df)

                if not unmatched_in_chunk_df.empty and len(unmatched_samples) < MAX_SAMPLES_TO_LOG:
                    unique_unmatched_tuples = set(unmatched_in_chunk_df[source_multi_cols].itertuples(index=False, name=None))
                    unmatched_samples.update(unique_unmatched_tuples)

                final_chunk_df = exploded_df.dropna(subset=[assoc_lookup_col])
                final_chunk_df = final_chunk_df[[source_id_col, assoc_lookup_col]]
                final_chunk_df.columns = [assoc_source_col, assoc_lookup_col]

                if not final_chunk_df.empty:
                    final_chunk_df.to_sql(name=assoc_table, con=engine, schema=schema_name, if_exists='append', index=False)

            if total_unmatched_count > 0:
                logger.debug(f"WARNING: A total of {total_unmatched_count} values from {source_table} could not find a match in {lookup_table} and were not inserted.")
                if unmatched_samples:
                    logger.debug(f"Sample of distinct unmatched source values (up to {MAX_SAMPLES_TO_LOG}):")
                    for sample in list(unmatched_samples)[:MAX_SAMPLES_TO_LOG]:
                         logger.debug(f"  - [{MULTI_COLUMN_SEPARATOR.join(map(str, sample))}]")

            # --- Finalizing Table ---
            logger.debug(f"Finalizing table {full_assoc_name}...")
            with engine.begin() as conn:
                temp_table_name = f"#{assoc_table}_temp_distinct"
                conn.execute(text(f"SELECT DISTINCT * INTO {temp_table_name} FROM {full_assoc_name};"))
                conn.execute(text(f"TRUNCATE TABLE {full_assoc_name};"))
                conn.execute(text(f"INSERT INTO {full_assoc_name} SELECT * FROM {temp_table_name};"))
                conn.execute(text(f"DROP TABLE {temp_table_name};"))

                pk_name = f"PK_{assoc_table}"
                fk_source_name = f"FK_{assoc_table}_{source_table}"
                fk_lookup_name = f"FK_{assoc_table}_{lookup_table}"

                conn.execute(text(f'ALTER TABLE {full_assoc_name} ALTER COLUMN "{assoc_source_col}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ALTER COLUMN "{assoc_lookup_col}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{pk_name}" PRIMARY KEY ("{assoc_source_col}", "{assoc_lookup_col}");'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{fk_source_name}" FOREIGN KEY ("{assoc_source_col}") REFERENCES "{schema_name}"."{source_table}"("{source_id_col}");'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{fk_lookup_name}" FOREIGN KEY ("{assoc_lookup_col}") REFERENCES "{schema_name}"."{lookup_table}"("{lookup_id_col}");'))

            logger.debug(f"Successfully created and populated {full_assoc_name}.")

        except Exception as e:
            logger.exception(f"An error occurred while processing the relation for {source_table}")

def implement_one_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/one_to_many_relations.csv"):
    """
    Implements one-to-many relations with case-insensitive and whitespace-insensitive joins.
    """
    MULTI_COLUMN_SEPARATOR = '-'
    try:
        relations_df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Could not load one-to-many relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)

    # --- HELPER FOR CLEANING SQL STRINGS ---
    # This template replaces non-breaking spaces, carriage returns, and line feeds.
    def sql_clean_string(column_ref):
        # Using NVARCHAR(MAX) as you mentioned the columns are nvarchar
        return f"LOWER(TRIM(REPLACE(REPLACE(REPLACE(CAST({column_ref} AS NVARCHAR(MAX)), CHAR(13), ''), CHAR(10), ''), CHAR(160), ' ')))"

    with engine.begin() as conn:
        for _, row in relations_df.iterrows():
            table_name = str(row["table_name"]).strip()
            source_lookup_cols_str = str(row["column_name"]).strip()
            replaced_with = str(row["replaced_with"]).strip()
            referenced_table = str(row["referenced_table"]).strip()
            referenced_column = str(row["referenced_column"]).strip()
            ref_lookup_cols_str = str(row["lookup_column"]).strip()

            source_columns = [c.strip() for c in source_lookup_cols_str.split(MULTI_COLUMN_SEPARATOR)]
            lookup_columns = [c.strip() for c in ref_lookup_cols_str.split(MULTI_COLUMN_SEPARATOR)]

            if len(source_columns) != len(lookup_columns):
                logger.error(
                    f"Mismatched number of lookup columns for table '{table_name}'. "
                    f"Source has {len(source_columns)} but Referenced has {len(lookup_columns)}. Skipping."
                )
                continue

            logger.debug(f"Processing one-to-many: {schema_name}.{table_name}...")

            try:
                # Check if the new ID column already exists
                check_col_exists_query = text(f"""
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table AND COLUMN_NAME = :col
                """)
                col_exists = conn.execute(check_col_exists_query, {"schema": schema_name, "table": table_name, "col": replaced_with}).fetchone()

                if not col_exists:
                    conn.execute(text(f'ALTER TABLE "{schema_name}"."{table_name}" ADD "{replaced_with}" INT NULL'))
                    logger.debug(f"Added column {replaced_with} to {schema_name}.{table_name}")
                else:
                    logger.debug(f"Column {replaced_with} already exists in {schema_name}.{table_name}.")

                # Build join conditions in a readable, error-free way
                conditions = []
                for sc, lc in zip(source_columns, lookup_columns):
                    src_ref = f'src."{sc}"'
                    ref_ref = f'ref."{lc}"'
                    condition = f"{sql_clean_string(src_ref)} = {sql_clean_string(ref_ref)}"
                    conditions.append(condition)
                join_conditions = " AND ".join(conditions)

                # Build and execute the main update query
                update_query = text(f"""
                    UPDATE src SET "{replaced_with}" = ref."{referenced_column}"
                    FROM "{schema_name}"."{table_name}" AS src
                    JOIN "{schema_name}"."{referenced_table}" AS ref ON {join_conditions}
                    WHERE src."{replaced_with}" IS NULL;
                """)
                result = conn.execute(update_query)
                logger.debug(f"Updated {replaced_with} in {schema_name}.{table_name}. Rows affected: {result.rowcount}")

                # Check for any values that were not matched
                unmatched_check_conditions = " AND ".join([
                    f"({col} IS NOT NULL AND TRIM(CAST({col} AS NVARCHAR(MAX))) <> '')" for col in source_columns
                ])
                unmatched_query = text(f"""
                    SELECT COUNT(*) AS unmatched_count
                    FROM "{schema_name}"."{table_name}"
                    WHERE "{replaced_with}" IS NULL AND ({unmatched_check_conditions});
                """)
                unmatched_count = conn.execute(unmatched_query).scalar_one()

                if unmatched_count > 0:
                    logger.debug(f"WARNING-> {unmatched_count} rows in {table_name} had a value but could not find a match in {referenced_table}.")
                    # Safely try to get a sample of unmatched values
                    try:
                        source_cols_for_select = ", ".join([f'"{c}"' for c in source_columns])
                        sample_query = text(f"""
                            SELECT TOP 10 {source_cols_for_select}
                            FROM "{schema_name}"."{table_name}"
                            WHERE "{replaced_with}" IS NULL AND ({unmatched_check_conditions})
                            GROUP BY {source_cols_for_select}
                            ORDER BY {source_cols_for_select};
                        """)
                        sample_unmatched_values = conn.execute(sample_query).fetchall()
                        if sample_unmatched_values:
                            logger.debug("Sample of distinct unmatched source values:")
                            for sample in sample_unmatched_values:
                                logger.debug(f"  - [{MULTI_COLUMN_SEPARATOR.join(map(str, sample))}]")
                    except Exception as sample_e:
                        logger.error(f"Could not retrieve a sample of unmatched values: {sample_e}")

                # Add foreign key constraint
                # Truncating name to be safe for database identifier length limits
                fk_constraint_name = f"FK_{table_name}_{referenced_table}_{replaced_with}"[:128]
                check_fk_exists_query = text("""
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = :fk_name
                    AND TABLE_SCHEMA = :schema AND TABLE_NAME = :table;
                """)
                fk_exists = conn.execute(check_fk_exists_query, {"fk_name": fk_constraint_name, "schema": schema_name, "table": table_name}).fetchone()

                if not fk_exists:
                    add_fk_query = text(f"""
                        ALTER TABLE "{schema_name}"."{table_name}"
                        ADD CONSTRAINT "{fk_constraint_name}"
                        FOREIGN KEY ("{replaced_with}") REFERENCES "{schema_name}"."{referenced_table}"("{referenced_column}")
                        ON DELETE NO ACTION ON UPDATE NO ACTION;
                    """)
                    conn.execute(add_fk_query)
                    logger.debug(f"Added foreign key {fk_constraint_name} on {replaced_with} in {schema_name}.{table_name}.")
                else:
                    logger.debug(f"Foreign key {fk_constraint_name} already exists on {schema_name}.{table_name}.")

            except Exception as e:
                logger.error(f"Failed processing {schema_name}.{table_name} with key {source_lookup_cols_str}: {e}")
                raise # Re-raise the exception to halt execution if one relation fails