import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger
from utils.utils import load_schema_changes

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
                            logger.warning(f"Table {schema_name}.{table_name} has {row_count} rows. Cannot easily convert column {column_name} to identity. Continuing without making it auto-increment.")
                        else:
                            # If table is empty, drop and recreate the column as identity
                            temp_column = f"{column_name}_temp"
                            conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} DROP COLUMN {column_name}"))
                            conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD {column_name} INT IDENTITY(1,1) NOT NULL"))
                            logger.debug(f"Recreated column {column_name} in table {schema_name}.{table_name} as identity column")
                    except Exception as e:
                        logger.error(f"Failed to set column {column_name} to auto-increment in table {schema_name}.{table_name}: {e}")
                        logger.warning(f"Continuing with primary key creation despite auto-increment failure")
            
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
    This function:
    1. Creates a new column with the referenced IDs
    2. Updates with correct IDs where references are found, NULL where not found
    3. Adds foreign key constraint with appropriate deletion action
    
    CSV columns:
    - table_name: The table to modify
    - column_name: The column containing values to replace
    - replaced_with: The new column name for the foreign key
    - referenced_table: The table containing the reference data
    - referenced_column: The ID column in the referenced table
    - lookup_column: The column in the referenced table to match values
    """
    relations_df = load_schema_changes(csv_path)
    engine = create_engine(target_db_url)
    
    # Track tables with foreign keys to handle multiple references to the same table
    table_references = {}
    
    # First pass: collect all table relationships
    for _, row in relations_df.iterrows():
        table_name = row["table_name"]
        referenced_table = row["referenced_table"]
        
        if table_name not in table_references:
            table_references[table_name] = {}
        
        if referenced_table not in table_references[table_name]:
            table_references[table_name][referenced_table] = 0
            
        table_references[table_name][referenced_table] += 1
    
    with engine.begin() as conn:
        for _, row in relations_df.iterrows():
            table_name = row["table_name"]
            column_name = row["column_name"]
            replaced_with = row["replaced_with"]
            referenced_table = row["referenced_table"]
            referenced_column = row["referenced_column"]
            lookup_column = row["lookup_column"]
            try:
                # Step 1: Add the new column for the foreign key (allowing NULL)
                conn.execute(text(
                    f"ALTER TABLE {schema_name}.{table_name} "
                    f"ADD {replaced_with} INT NULL"
                ))
                logger.debug(f"Added column {replaced_with} to {schema_name}.{table_name}")
                
                # Step 2: Update the new column with referenced IDs
                update_query = f"""
                UPDATE src
                SET {replaced_with} = ref.{referenced_column}
                FROM {schema_name}.{table_name} src
                LEFT JOIN {schema_name}.{referenced_table} ref
                    ON src.{column_name} = ref.{lookup_column}
                """
                conn.execute(text(update_query))
                
                # Log the number of unmatched references
                unmatched_query = f"""
                SELECT COUNT(*) as unmatched_count
                FROM {schema_name}.{table_name}
                WHERE {replaced_with} IS NULL
                """
                unmatched_count = conn.execute(text(unmatched_query)).scalar()
                if unmatched_count:
                    logger.debug(f"Found {unmatched_count} unmatched references in {schema_name}.{table_name}.{column_name}")
                
                # Step 3: Add foreign key constraint with appropriate deletion action
                # Use NO ACTION for multiple references to the same table to avoid cascade cycles
                delete_action = "NO ACTION" if table_references[table_name].get(referenced_table, 0) > 1 else "SET NULL"
                
                add_fk_query = f"""
                ALTER TABLE {schema_name}.{table_name}
                ADD CONSTRAINT FK_{table_name}_{replaced_with}
                FOREIGN KEY ({replaced_with}) REFERENCES {schema_name}.{referenced_table}({referenced_column})
                ON DELETE {delete_action}
                """
                conn.execute(text(add_fk_query))
                logger.debug(f"Added foreign key constraint on {replaced_with} in {schema_name}.{table_name} with ON DELETE {delete_action}")
                
            except Exception as e:
                logger.error(f"Failed to implement one-to-many relation for {schema_name}.{table_name}.{column_name}: {e}")
                raise e

def implement_many_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/many_to_many_relations.csv"):
    """
    Implement many-to-many relations based on CSV configuration.
    This function:
    1. Creates associative tables if they don't exist
    2. Populates the associative tables with correct relationships
    3. Logs unmatched values for later investigation
    
    CSV columns:
    - source_table: The table containing the semi-colon separated values
    - source_id_column: The ID column in the source table
    - source_multi_column: The column containing semi-colon separated values
    - lookup_table: The table containing the reference entities
    - lookup_name_column: The column with names to match in the lookup table
    - lookup_id_column: The ID column in the lookup table
    - associative_table: The name for the new associative table
    - assoc_source_column: Foreign key column for the source table
    - assoc_lookup_column: Foreign key column for the lookup table
    """
    relations_df = load_schema_changes(csv_path)
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        for _, row in relations_df.iterrows():
            source_table = row["source_table"]
            source_id_column = row["source_id_column"]
            source_multi_column = row["source_multi_column"]
            lookup_table = row["lookup_table"]
            lookup_name_column = row["lookup_name_column"]
            lookup_id_column = row["lookup_id_column"]
            associative_table = row["associative_table"]
            assoc_source_column = row["assoc_source_column"]
            assoc_lookup_column = row["assoc_lookup_column"]
            try:
                # Create associative table if it doesn't exist
                # Note: In many-to-many relationships, we typically don't allow NULLs
                # in the associative table - we simply don't create the relationship
                create_table_query = f"""
                IF OBJECT_ID('{schema_name}.{associative_table}', 'U') IS NULL
                BEGIN
                    CREATE TABLE {schema_name}.{associative_table} (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        {assoc_source_column} INT NOT NULL,
                        {assoc_lookup_column} INT NOT NULL,
                        CONSTRAINT FK_{associative_table}_{source_table} 
                            FOREIGN KEY ({assoc_source_column}) 
                            REFERENCES {schema_name}.{source_table}({source_id_column}),
                        CONSTRAINT FK_{associative_table}_{lookup_table} 
                            FOREIGN KEY ({assoc_lookup_column}) 
                            REFERENCES {schema_name}.{lookup_table}({lookup_id_column})
                    )
                END
                """
                conn.execute(text(create_table_query))
                logger.debug(f"Created associative table {schema_name}.{associative_table} if it didn't exist")
                
                # Get all source rows with their multi-values
                source_data_query = f"""
                SELECT {source_id_column}, {source_multi_column}
                FROM {schema_name}.{source_table}
                WHERE {source_multi_column} IS NOT NULL AND {source_multi_column} != ''
                """
                source_data = pd.read_sql(source_data_query, conn)
                
                # Get all lookup values for mapping
                lookup_data_query = f"""
                SELECT {lookup_id_column}, {lookup_name_column}
                FROM {schema_name}.{lookup_table}
                """
                lookup_data = pd.read_sql(lookup_data_query, conn)
                lookup_dict = dict(zip(lookup_data[lookup_name_column], lookup_data[lookup_id_column]))
                
                # Track unmatched values for logging
                unmatched_values = []
                
                # For each source row, create entries in the associative table
                for _, src_row in source_data.iterrows():
                    source_id = src_row[source_id_column]
                    values_str = src_row[source_multi_column]
                    
                    # Split by semicolon and strip whitespace
                    if values_str and isinstance(values_str, str):
                        values = [v.strip() for v in values_str.split(';')]
                        
                        for value in values:
                            if value:
                                lookup_id = lookup_dict.get(value)
                                if lookup_id is not None:
                                    # Check if the relationship already exists
                                    check_query = f"""
                                    SELECT 1 FROM {schema_name}.{associative_table}
                                    WHERE {assoc_source_column} = {source_id} AND {assoc_lookup_column} = {lookup_id}
                                    """
                                    exists = conn.execute(text(check_query)).fetchone()
                                    
                                    if not exists:
                                        # Insert the new relationship
                                        insert_query = f"""
                                        INSERT INTO {schema_name}.{associative_table} ({assoc_source_column}, {assoc_lookup_column})
                                        VALUES ({source_id}, {lookup_id})
                                        """
                                        conn.execute(text(insert_query))
                                else:
                                    # Track unmatched values for later investigation
                                    unmatched_values.append(value)
                                    # logger.debug(f"Skipping unmatched value '{value}' for {source_id} in {source_table}.{source_multi_column}")
                
                # Log unmatched values
                if unmatched_values:
                    unique_unmatched = list(set(unmatched_values))
                    logger.debug(f"Found {len(unique_unmatched)} unique unmatched values in {source_table}.{source_multi_column}")
                    # logger.debug(f"Unmatched values: {unique_unmatched}")
                
            except Exception as e:
                logger.error(f"Failed to implement many-to-many relation for {schema_name}.{source_table}.{source_multi_column}: {e}")
                raise e