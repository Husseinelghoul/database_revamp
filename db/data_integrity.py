import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger
from utils.utils import generate_constraint_name, load_schema_changes

logger = setup_logger()

def add_primary_keys(target_db_url, schema_name, csv_path="config/data_integrity_changes/primary_keys.csv"):
    """
    Add primary keys specified in the CSV file, checking if they already exist first.
    Makes columns NOT NULL if they are nullable.
    Makes integer columns auto-incrementing if they aren't already.
    """
    primary_keys_df = load_schema_changes(csv_path)
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in primary_keys_df.iterrows():
            table_name = row["table_name"]
            column_name = row["column_name"]
            
            # Check column data type and nullability
            column_info_query = text(f"""
                SELECT data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table
                AND column_name = :column
            """)
            
            column_info = conn.execute(
                column_info_query,
                {"schema": schema_name, "table": table_name, "column": column_name}
            ).fetchone()
            
            if not column_info:
                logger.error(f"Column {column_name} not found in table {schema_name}.{table_name}")
                continue
                
            data_type, is_nullable = column_info
            
            # Make column NOT NULL if it's nullable (SQL Server syntax)
            if is_nullable == 'YES':
                logger.debug(f"Column {column_name} in table {schema_name}.{table_name} is nullable, making it NOT NULL")
                try:
                    # SQL Server syntax for altering column to NOT NULL
                    # We need to include the data type when changing nullability
                    conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {column_name} {data_type} NOT NULL"))
                    logger.debug(f"Set column {column_name} in table {schema_name}.{table_name} to NOT NULL")
                except Exception as e:
                    logger.error(f"Failed to set column {column_name} to NOT NULL in table {schema_name}.{table_name}: {e}")
                    raise e
            
            # Check if integer column is already auto-incrementing (SQL Server specific)
            is_integer = data_type in ('integer', 'int', 'bigint', 'smallint')
            is_identity = False
            
            if is_integer:
                # SQL Server-specific query to check if column is identity
                identity_check_query = text(f"""
                    SELECT COUNT(*)
                    FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = :schema
                    AND t.name = :table
                    AND c.name = :column
                    AND c.is_identity = 1
                """)
                
                is_identity = conn.execute(
                    identity_check_query,
                    {"schema": schema_name, "table": table_name, "column": column_name}
                ).scalar() > 0
                
                # Make integer column auto-incrementing if it's not already (SQL Server syntax)
                if not is_identity:
                    logger.debug(f"Column {column_name} in table {schema_name}.{table_name} is integer but not auto-incrementing, making it auto-increment")
                    try:
                        # For SQL Server, we need to create a new identity column and replace the old one
                        # This is complex as we need to:
                        # 1. Create a temp column with identity
                        # 2. Copy data
                        # 3. Drop original column
                        # 4. Rename temp column
                        
                        # Check if there's data in the table
                        row_count_query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                        row_count = conn.execute(row_count_query).scalar()
                        
                        if row_count > 0:
                            logger.debug(f"Table {schema_name}.{table_name} has {row_count} rows. Cannot easily convert column {column_name} to identity. Continuing without making it auto-increment.")
                        else:
                            # If table is empty, drop and recreate the column as identity
                            temp_column = f"{column_name}_temp"
                            conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} DROP COLUMN {column_name}"))
                            conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD {column_name} INT IDENTITY(1,1) NOT NULL"))
                            logger.debug(f"Recreated column {column_name} in table {schema_name}.{table_name} as identity column")
                    except Exception as e:
                        logger.error(f"Failed to set column {column_name} to auto-increment in table {schema_name}.{table_name}: {e}")
                        logger.debug(f"Continuing with primary key creation despite auto-increment failure")
                        raise e
            
            # Query to check if primary key already exists
            pk_check_query = text(f"""
                SELECT COUNT(*) 
                FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' 
                AND table_schema = :schema 
                AND table_name = :table
            """)
            
            pk_exists = conn.execute(
                pk_check_query, 
                {"schema": schema_name, "table": table_name}
            ).scalar() > 0
            
            if pk_exists:
                logger.debug(f"Primary key already exists on table {schema_name}.{table_name}, removing it first")
                try:
                    # First we need to get the constraint name
                    constraint_query = text(f"""
                        SELECT constraint_name
                        FROM information_schema.table_constraints
                        WHERE constraint_type = 'PRIMARY KEY'
                        AND table_schema = :schema
                        AND table_name = :table
                    """)
                    
                    constraint_name = conn.execute(
                        constraint_query,
                        {"schema": schema_name, "table": table_name}
                    ).scalar()
                    
                    # Drop the existing primary key
                    conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} DROP CONSTRAINT {constraint_name}"))
                    logger.debug(f"Removed existing primary key constraint {constraint_name} from {schema_name}.{table_name}")
                except Exception as e:
                    logger.error(f"Failed to remove existing primary key on table {schema_name}.{table_name}: {e}")
                    raise e
            
            # Add the primary key
            try:
                conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({column_name})"))
                logger.debug(f"Added primary key on column {column_name} for table {schema_name}.{table_name}")
            except Exception as e:
                logger.error(f"Failed to add primary key on column {column_name} for table {schema_name}.{table_name}: {e}")
                raise e

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
