import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy import create_engine, text

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
    Implement many-to-many relations based on CSV configuration.
    1. Creates associative tables if they don't exist.
    2. Populates associative tables.
    3. Logs unmatched values.
    
    CSV: source_table, source_id_column, source_multi_column, lookup_table, 
         lookup_name_column, lookup_id_column, associative_table, 
         assoc_source_column, assoc_lookup_column
    """
    try:
        relations_df = load_schema_changes(csv_path) # Ensure this function is defined
    except Exception as e:
        logger.error(f"Could not load many-to-many relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        for _, row in relations_df.iterrows():
            source_table = str(row["source_table"]).strip()
            source_id_column = str(row["source_id_column"]).strip()
            source_multi_column = str(row["source_multi_column"]).strip()
            lookup_table = str(row["lookup_table"]).strip()
            lookup_name_column = str(row["lookup_name_column"]).strip()
            lookup_id_column = str(row["lookup_id_column"]).strip()
            associative_table = str(row["associative_table"]).strip()
            assoc_source_column = str(row["assoc_source_column"]).strip() 
            assoc_lookup_column = str(row["assoc_lookup_column"]).strip()
            seperator = str(row["seperator"]).strip()

            logger.debug(f"Processing many-to-many: {schema_name}.{source_table} <-> {schema_name}.{lookup_table} via {schema_name}.{associative_table}")

            try:
                # Generate constraint names
                fk_assoc_to_source_name = generate_constraint_name(
                    prefix="FK",
                    name_elements=[associative_table, source_table, assoc_source_column]
                )
                fk_assoc_to_lookup_name = generate_constraint_name(
                    prefix="FK",
                    name_elements=[associative_table, lookup_table, assoc_lookup_column]
                )
                uq_constraint_name = generate_constraint_name(
                    prefix="UQ",
                    name_elements=[associative_table, assoc_source_column, assoc_lookup_column]
                )
                
                # Create associative table if it doesn't exist
                create_table_query = f"""
                IF OBJECT_ID('{schema_name}.{associative_table}', 'U') IS NULL
                BEGIN
                    CREATE TABLE {schema_name}.{associative_table} (
                        id INT IDENTITY(1,1) PRIMARY KEY, 
                        {assoc_source_column} INT NOT NULL,
                        {assoc_lookup_column} INT NOT NULL,
                        CONSTRAINT {fk_assoc_to_source_name}
                            FOREIGN KEY ({assoc_source_column}) REFERENCES {schema_name}.{source_table}({source_id_column})
                            ON DELETE NO ACTION ON UPDATE NO ACTION, 
                        CONSTRAINT {fk_assoc_to_lookup_name}
                            FOREIGN KEY ({assoc_lookup_column}) REFERENCES {schema_name}.{lookup_table}({lookup_id_column})
                            ON DELETE NO ACTION ON UPDATE NO ACTION,
                        CONSTRAINT {uq_constraint_name} 
                            UNIQUE ({assoc_source_column}, {assoc_lookup_column}) 
                    );
                    PRINT 'Created associative table {schema_name}.{associative_table}';
                END
                ELSE
                BEGIN
                    PRINT 'Associative table {schema_name}.{associative_table} already exists.';
                END
                """
                conn.execute(text(create_table_query))
                logger.debug(f"Ensured associative table {schema_name}.{associative_table} exists.")
                
                # Get all source rows with their multi-values to process
                source_data_query = f"""
                SELECT {source_id_column}, {source_multi_column}
                FROM {schema_name}.{source_table}
                WHERE {source_multi_column} IS NOT NULL AND LTRIM(RTRIM(CAST({source_multi_column} AS VARCHAR(MAX)))) <> '';
                """
                source_df = pd.read_sql_query(text(source_data_query), conn)
                
                # Get all lookup values for efficient mapping
                lookup_data_query = f"""
                SELECT {lookup_id_column}, {lookup_name_column}
                FROM {schema_name}.{lookup_table};
                """
                lookup_df = pd.read_sql_query(text(lookup_data_query), conn)
                # Create a dictionary for quick lookups: {lookup_name: lookup_id}
                lookup_dict = pd.Series(lookup_df[lookup_id_column].values, index=lookup_df[lookup_name_column].str.strip()).to_dict()
                
                unmatched_values_log = {} # To store source_id and the value that didn't match

                for _, src_row in source_df.iterrows():
                    source_id_val = src_row[source_id_column]
                    multi_value_str = src_row[source_multi_column]
                    
                    if pd.isna(multi_value_str) or not isinstance(multi_value_str, str):
                        continue

                    individual_values = [v.strip() for v in multi_value_str.split(seperator) if v.strip()]
                    
                    for value_to_lookup in individual_values:
                        # Normalize value_to_lookup if lookup_dict keys are normalized (e.g. .lower())
                        matched_lookup_id = lookup_dict.get(value_to_lookup) # Case-sensitive match by default
                        
                        if matched_lookup_id is not None:
                            # Insert into associative table, handling potential duplicates with IF NOT EXISTS
                            insert_sql = f"""
                            IF NOT EXISTS (
                                SELECT 1 FROM {schema_name}.{associative_table} 
                                WHERE {assoc_source_column} = {source_id_val} 
                                AND {assoc_lookup_column} = {matched_lookup_id}
                            )
                            BEGIN
                                INSERT INTO {schema_name}.{associative_table} ({assoc_source_column}, {assoc_lookup_column}) 
                                VALUES ({source_id_val}, {matched_lookup_id});
                            END
                            """
                            try:
                                conn.execute(text(insert_sql))
                            except Exception as insert_ex: # Catch specific insert errors if needed
                                logger.error(f"Error inserting ({source_id_val}, {matched_lookup_id}) into {associative_table}: {insert_ex}")
                        else:
                            if source_id_val not in unmatched_values_log:
                                unmatched_values_log[source_id_val] = []
                            if value_to_lookup not in unmatched_values_log[source_id_val]: # Avoid duplicate logging for same unmatched value per source_id
                                unmatched_values_log[source_id_val].append(value_to_lookup)
                
                logger.debug(f"Processed population of {schema_name}.{associative_table}.")

                if unmatched_values_log:
                    logger.debug(f"Found unmatched values during {schema_name}.{associative_table} population:")
                    for src_id, vals in unmatched_values_log.items():
                        logger.debug(f"  Source ID {src_id} (from {source_table}) had unmatched lookup values: {', '.join(vals)}")
                
            except Exception as e:
                logger.error(f"Failed for {schema_name}.{source_table} ({source_multi_column}) -> {schema_name}.{associative_table}: {e}")
                raise
