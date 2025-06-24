import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config.constants import PROCESSING_CHUNK_SIZE
from utils.logger import setup_logger
from utils.utils import generate_constraint_name, load_schema_changes

logger = setup_logger()

def add_primary_keys(target_db_url, schema_name):
    """
    Assertively ensures that for every table with an 'id' column, 'id' is the primary key,
    EXCLUDING a predefined list of infrastructure tables.
    """
    engine = create_engine(target_db_url)

    # The list of tables to completely ignore during the process.
    excluded_tables = (
        'app_user',
        'user_view_permission',
        'user_project',
        'user_entity',
        'sessions',
        'role_template'
    )

    # --- FIX APPLIED HERE ---
    # Instead of passing the tuple as a parameter, we format it directly into the SQL string.
    # This is safe because the list is hardcoded and not from user input.
    # This avoids the pyodbc driver's TVP (Table-Valued Parameter) bug.
    excluded_tables_sql_str = ", ".join([f"'{table}'" for table in excluded_tables])

    discovery_sql = text(f"""
        SELECT
            t.TABLE_NAME,
            c.COLUMN_NAME AS IdColumnName,
            pk.ConstraintName,
            pk.PkColumnName
        FROM
            INFORMATION_SCHEMA.TABLES t
        INNER JOIN
            INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        LEFT JOIN (
            SELECT
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME,
                tc.CONSTRAINT_NAME AS ConstraintName,
                kcu.COLUMN_NAME AS PkColumnName
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE
                tc.TABLE_SCHEMA = :schema_name
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) AS pk ON t.TABLE_SCHEMA = pk.TABLE_SCHEMA AND t.TABLE_NAME = pk.TABLE_NAME
        WHERE
            t.TABLE_SCHEMA = :schema_name
            AND t.TABLE_TYPE = 'BASE TABLE'
            AND LOWER(c.COLUMN_NAME) = 'id'
            AND t.TABLE_NAME NOT IN ({excluded_tables_sql_str});
    """)

    try:
        with engine.connect() as conn:
            # We no longer pass the 'excluded_tables' tuple as a parameter.
            results = conn.execute(discovery_sql, {"schema_name": schema_name}).fetchall()
            tables_to_process = [(row[0], row[1], row[2], row[3]) for row in results]
    except SQLAlchemyError as e:
        logger.exception(f"Fatal: Failed to query schema information.")
        return

    if not tables_to_process:
        logger.debug(f"No tables requiring primary key changes were found in schema '{schema_name}' (after exclusions). No action needed.")
        return

    logger.debug(f"Found {len(tables_to_process)} tables that need a primary key on their 'id' column.")

    # The logic below this point remains the same and is correct.
    with engine.begin() as conn:
        for table_name, id_column_name, pk_constraint_name, pk_column_name in tables_to_process:
            full_table_name = f'"{schema_name}"."{table_name}"'
            
            if pk_constraint_name and pk_column_name.lower() == id_column_name.lower():
                logger.debug(f"OK: Primary key on {full_table_name} is already correctly set to '{id_column_name}'.")
                continue

            try:
                if pk_constraint_name:
                    logger.debug(f"FIXING: {full_table_name} has a PK '{pk_constraint_name}' on the wrong column ('{pk_column_name}'). Dropping it.")
                    conn.execute(text(f'ALTER TABLE {full_table_name} DROP CONSTRAINT "{pk_constraint_name}";'))

                logger.debug(f"ACTION: Adding primary key to {full_table_name} on column '{id_column_name}'...")
                
                conn.execute(text(f'ALTER TABLE {full_table_name} ALTER COLUMN "{id_column_name}" INT NOT NULL;'))
                
                conn.execute(text(f'ALTER TABLE {full_table_name} ADD PRIMARY KEY ("{id_column_name}");'))
                logger.debug(f"SUCCESS: Primary key for {full_table_name} is now set on '{id_column_name}'.")

            except SQLAlchemyError:
                logger.exception(f"FAILED: Could not set primary key for {full_table_name}. The 'id' column might contain NULLs or duplicate values. Skipping this table.")

def implement_one_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/one_to_many_relations.csv"):
    """
    Implement one-to-many relations based on CSV configuration.
    1. Adds a new column for the foreign key ID.
    2. Populates the new column with IDs from the referenced table.
    3. Adds a foreign key constraint with ON DELETE NO ACTION.
    
    CSV: table_name, column_name, replaced_with, referenced_table, referenced_column, lookup_column
    """
    try:
        relations_df = load_schema_changes(csv_path) # Ensure this function is defined
    except Exception as e:
        logger.error(f"Could not load one-to-many relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)
    
    with engine.begin() as conn: 
        for _, row in relations_df.iterrows():
            table_name = str(row["table_name"]).strip()
            column_name = str(row["column_name"]).strip() 
            replaced_with = str(row["replaced_with"]).strip() 
            referenced_table = str(row["referenced_table"]).strip()
            referenced_column = str(row["referenced_column"]).strip() 
            lookup_column = str(row["lookup_column"]).strip() 

            logger.debug(f"Processing one-to-many: {schema_name}.{table_name}.{replaced_with} -> {schema_name}.{referenced_table}.{referenced_column}")
            
            try:
                # Check if the new FK column already exists
                check_col_exists_query = f"""
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{replaced_with}'
                """
                col_exists = conn.execute(text(check_col_exists_query)).fetchone()

                if not col_exists:
                    # Add the new column for the foreign key (allowing NULL initially)
                    conn.execute(text(
                        f"ALTER TABLE {schema_name}.{table_name} ADD {replaced_with} INT NULL" # Assuming integer IDs
                    ))
                    logger.debug(f"Added column {replaced_with} to {schema_name}.{table_name}")
                else:
                    logger.debug(f"Column {replaced_with} already exists in {schema_name}.{table_name}.")

                # Update the new column with referenced IDs
                update_query = f"""
                UPDATE src
                SET src.{replaced_with} = ref.{referenced_column}
                FROM {schema_name}.{table_name} AS src
                LEFT JOIN {schema_name}.{referenced_table} AS ref
                    ON src.{column_name} = ref.{lookup_column}
                WHERE src.{replaced_with} IS NULL;  -- Optionally, only update if not already set
                """
                result = conn.execute(text(update_query))
                logger.debug(f"Updated {replaced_with} in {schema_name}.{table_name}. Rows affected: {result.rowcount}")
                
                # Log the number of still unmatched references
                unmatched_query = f"""
                SELECT COUNT(*) AS unmatched_count
                FROM {schema_name}.{table_name}
                WHERE {replaced_with} IS NULL 
                AND {column_name} IS NOT NULL AND LTRIM(RTRIM(CAST({column_name} AS VARCHAR(MAX)))) <> '';
                """
                unmatched_count = conn.execute(text(unmatched_query)).scalar_one_or_none()
                if unmatched_count and unmatched_count > 0:
                    logger.debug(f"{unmatched_count} entries in {schema_name}.{table_name} could not find a match in {schema_name}.{referenced_table} via {column_name} -> {lookup_column}. {replaced_with} remains NULL for these.")
                
                # Add foreign key constraint
                delete_action = "NO ACTION" 
                fk_constraint_name = generate_constraint_name(
                    prefix="FK",
                    name_elements=[table_name, referenced_table, replaced_with]
                )

                # Check if FK constraint already exists
                check_fk_exists_query = f"""
                SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '{fk_constraint_name}'
                AND TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';
                """
                fk_exists = conn.execute(text(check_fk_exists_query)).fetchone()

                if not fk_exists:
                    add_fk_query = f"""
                    ALTER TABLE {schema_name}.{table_name}
                    ADD CONSTRAINT {fk_constraint_name}
                    FOREIGN KEY ({replaced_with}) REFERENCES {schema_name}.{referenced_table}({referenced_column})
                    ON DELETE {delete_action}
                    ON UPDATE NO ACTION;
                    """
                    conn.execute(text(add_fk_query))
                    logger.debug(f"Added foreign key {fk_constraint_name} on {replaced_with} in {schema_name}.{table_name} with ON DELETE {delete_action}.")
                else:
                    logger.debug(f"Foreign key {fk_constraint_name} already exists on {schema_name}.{table_name}.")
                
            except Exception as e:
                logger.error(f"Failed for {schema_name}.{table_name}.{column_name} -> {replaced_with}: {e}")
                raise 

def implement_many_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/many_to_many_relations.csv"):
    """
    A robust, memory-efficient implementation that creates many-to-many relations
    using batch processing for the source table to handle huge datasets.
    """
    # This defines how many rows to process from the source table at a time.
    
    try:
        relations_df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Could not load relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)
    
    for _, row in relations_df.iterrows():
        # Read all config from the row
        source_table, source_id_col, source_multi_col = row["source_table"], row["source_id_column"], row["source_multi_column"]
        lookup_table, lookup_name_col, lookup_id_col = row["lookup_table"], row["lookup_name_column"], row["lookup_id_column"]
        assoc_table, assoc_source_col, assoc_lookup_col = row["associative_table"], row["assoc_source_column"], row["assoc_lookup_column"]
        separator = row["seperator"]

        full_source_name = f'"{schema_name}"."{source_table}"'
        full_lookup_name = f'"{schema_name}"."{lookup_table}"'
        full_assoc_name = f'"{schema_name}"."{assoc_table}"'
        logger.debug(f"Processing relation for {full_assoc_name}...")

        try:
            # === Step 1: Pre-load the entire lookup table into an in-memory dictionary ===
            # This is fast and assumes the lookup table itself is not billions of rows.
            logger.debug(f"Caching lookup table: {full_lookup_name}...")
            lookup_sql = f'SELECT "{lookup_id_col}", "{lookup_name_col}" FROM {full_lookup_name}'
            lookup_df = pd.read_sql(lookup_sql, engine)
            # Create a dictionary for instant lookups: {lookup_name: lookup_id}
            lookup_dict = pd.Series(lookup_df[lookup_id_col].values, index=lookup_df[lookup_name_col].str.strip()).to_dict()
            logger.debug(f"Cached {len(lookup_dict)} lookup values.")

            # === Step 2: Setup a fresh, empty associative table without constraints ===
            # Constraints will be added at the end for much better insert performance.
            with engine.begin() as conn:
                logger.debug(f"Recreating empty target table: {full_assoc_name}")
                conn.execute(text(f'DROP TABLE IF EXISTS {full_assoc_name};'))
                create_sql = f"""
                CREATE TABLE {full_assoc_name} (
                    "{assoc_source_col}" INT,
                    "{assoc_lookup_col}" INT
                );
                """
                conn.execute(text(create_sql))

            # === Step 3: Stream the HUGE source table in chunks and process each one ===
            logger.debug(f"Streaming source table {full_source_name} in chunks of {PROCESSING_CHUNK_SIZE}...")
            source_sql = f'SELECT "{source_id_col}", "{source_multi_col}" FROM {full_source_name} WHERE "{source_multi_col}" IS NOT NULL'
            
            total_rows_processed = 0
            for source_chunk_df in pd.read_sql(source_sql, engine, chunksize=PROCESSING_CHUNK_SIZE):
                
                total_rows_processed += len(source_chunk_df)
                logger.debug(f"  Processing source rows chunk... (up to row {total_rows_processed})")

                # Explode the current chunk
                source_chunk_df[source_multi_col] = source_chunk_df[source_multi_col].str.split(separator)
                exploded_df = source_chunk_df.explode(source_multi_col)
                exploded_df[source_multi_col] = exploded_df[source_multi_col].str.strip()
                
                # Use the fast in-memory dictionary to map names to IDs
                exploded_df[assoc_lookup_col] = exploded_df[source_multi_col].map(lookup_dict)
                
                # Filter out any names that didn't have a match in the lookup dict
                final_chunk_df = exploded_df.dropna(subset=[assoc_lookup_col])
                
                # Prepare the final DataFrame for insertion
                final_chunk_df = final_chunk_df[[source_id_col, assoc_lookup_col]]
                final_chunk_df.columns = [assoc_source_col, assoc_lookup_col]
                
                # Append the processed chunk to the database
                if not final_chunk_df.empty:
                    final_chunk_df.to_sql(name=assoc_table, con=engine, schema=schema_name, if_exists='append', index=False)
            
            # === Step 4: Finalize the table (remove duplicates and add constraints) ===
            logger.debug(f"Finalizing table {full_assoc_name} by removing duplicates and adding constraints...")
            with engine.begin() as conn:
                # Create a temporary table with distinct rows
                temp_table_name = f"#{assoc_table}_temp_distinct"
                conn.execute(text(f"SELECT DISTINCT * INTO {temp_table_name} FROM {full_assoc_name};"))
                # Truncate the original table and insert the distinct rows back
                conn.execute(text(f"TRUNCATE TABLE {full_assoc_name};"))
                conn.execute(text(f"INSERT INTO {full_assoc_name} SELECT * FROM {temp_table_name};"))
                conn.execute(text(f"DROP TABLE {temp_table_name};"))

                # Now that the data is clean and unique, add the keys
                pk_name = f"PK_{assoc_table}"
                fk_source_name = f"FK_{assoc_table}_{source_table}"
                fk_lookup_name = f"FK_{assoc_table}_{lookup_table}"

                conn.execute(text(f'ALTER TABLE {full_assoc_name} ALTER COLUMN "{assoc_source_col}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ALTER COLUMN "{assoc_lookup_col}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{pk_name}" PRIMARY KEY ("{assoc_source_col}", "{assoc_lookup_col}");'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{fk_source_name}" FOREIGN KEY ("{assoc_source_col}") REFERENCES "{schema_name}"."{source_table}"("{source_id_col}");'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{fk_lookup_name}" FOREIGN KEY ("{assoc_lookup_col}") REFERENCES "{schema_name}"."{lookup_table}"("{lookup_id_col}");'))

            logger.debug(f"Successfully created and populated {full_assoc_name} with {total_rows_processed} source rows processed.")

        except Exception as e:
            logger.exception(f"An error occurred while processing the relation for {source_table}")
