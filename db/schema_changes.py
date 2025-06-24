from sqlalchemy import create_engine, text
from itertools import product
import pandas as pd

from utils.logger import setup_logger
from utils.utils import load_schema_changes

logger = setup_logger()

def split_columns(target_db_url, schema_name, csv_path="config/schema_changes/column_splits.csv"):
    """
    Create new columns by copying data from source columns in MSSQL, as specified in the CSV file.
    
    The CSV file should have the following columns:
    - table_name: Name of the table where the columns exist.
    - source_column: Name of the column to copy data from.
    - new_column: Name of the new column to be created.
    """
    # Load the schema changes from the CSV file
    splits_df = load_schema_changes(csv_path)
    # Connect to the database
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in splits_df.iterrows():
            table_name = row['table_name']
            source_column = row['source_column']
            new_column = row['new_column']
            
            try:
                # Get the data type, length, precision, and scale of the source column
                result = conn.execute(text(f"""
                    SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name AND COLUMN_NAME = :source_column;
                """), {"schema_name": schema_name, "table_name": table_name, "source_column": source_column})
                
                column_info = result.fetchone()
                if not column_info:
                    raise ValueError(f"Column {source_column} not found in {schema_name}.{table_name}.")
                
                # Extract the column details
                data_type = column_info[0]
                char_max_length = column_info[1]
                numeric_precision = column_info[2]
                numeric_scale = column_info[3]
                
                # Construct the column definition
                if data_type.upper() in ["VARCHAR", "NVARCHAR", "CHAR", "NCHAR"]:
                    if char_max_length == -1:  # Handle MAX for VARCHAR(MAX), NVARCHAR(MAX)
                        column_definition = f"{data_type}(MAX)"
                    else:
                        column_definition = f"{data_type}({char_max_length})"
                elif data_type in ["DECIMAL", "NUMERIC"]:
                    column_definition = f"{data_type}({numeric_precision}, {numeric_scale})"
                else:
                    column_definition = data_type  # For other types like INT, DATE, etc.
                # Create the new column with the constructed definition
                conn.execute(text(f"""
                    ALTER TABLE {schema_name}.{table_name}
                    ADD {new_column} {column_definition};
                """))
                
                # Copy data from the source column to the new column
                conn.execute(text(f"""
                    UPDATE {schema_name}.{table_name}
                    SET {new_column} = {source_column};
                """))
                
                logger.debug(f"Created column {new_column} in {schema_name}.{table_name} with type {column_definition} and copied data from {source_column}.")
            except Exception as e:
                logger.error(f"Failed to create column {new_column} in {schema_name}.{table_name}: {e}")

def split_tables(target_db_url, schema_name, csv_path="config/schema_changes/table_splits.csv"):
    """
    A robust, memory-efficient version that splits tables using batch processing
    and a generic schema for the target table.
    """
    # This defines how many rows to process from the source table at a time.
    CHUNK_SIZE = 1000
    
    try:
        splits_df = pd.read_csv(csv_path)
    except FileNotFoundError:
        # A critical failure like a missing config should remain an error.
        logger.error(f"Configuration file not found at: {csv_path}")
        return
        
    engine = create_engine(target_db_url)
    
    for _, config_row in splits_df.iterrows():
        source_table_name = config_row['source_table_name']
        columns_to_copy = [col.strip() for col in config_row['columns_to_copy'].split(';')]
        columns_to_split = [col.strip() for col in config_row['columns_to_split'].split(';')]
        separator = config_row['separator']
        target_table_name = config_row['target_table_name']
        all_source_columns = columns_to_copy + columns_to_split
        
        full_source_name = f"\"{schema_name}\".\"{source_table_name}\""
        full_target_name = f"\"{schema_name}\".\"{target_table_name}\""

        logger.debug(f"Processing split from {full_source_name} to {full_target_name}...")

        try:
            # === Step 1: One-time setup of the target table with a generic schema ===
            with engine.begin() as conn:
                logger.debug(f"Setting up fresh target table {full_target_name}...")
                
                # Drop the target table if it exists
                conn.execute(text(f'DROP TABLE IF EXISTS {full_target_name};'))
                
                # Create a generic definition for all columns as NVARCHAR(MAX)
                col_definitions_str = ', '.join([f'"{col}" NVARCHAR(MAX) NULL' for col in all_source_columns])

                # Create the table with the simplified, generic schema
                create_sql = f"""
                    CREATE TABLE {full_target_name} (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        {col_definitions_str}
                    );
                """
                conn.execute(text(create_sql))
            
            # === Step 2: Process source table in streaming chunks ===
            logger.debug(f"Reading source table in chunks of {CHUNK_SIZE} rows...")
            select_cols_str = ', '.join([f'"{col}"' for col in all_source_columns])
            sql_query = f'SELECT {select_cols_str} FROM {full_source_name}'
            
            total_rows_processed = 0
            # pd.read_sql_query with chunksize returns an iterator
            for source_chunk_df in pd.read_sql_query(sql_query, engine, chunksize=CHUNK_SIZE):
                
                chunk_start = total_rows_processed + 1
                total_rows_processed += len(source_chunk_df)
                logger.debug(f"  Processing source rows {chunk_start} to {total_rows_processed}...")

                # Normalize the current chunk
                normalized_rows = []
                for _, source_row in source_chunk_df.iterrows():
                    split_values_lists = [
                        [v.strip() for v in str(source_row[col]).split(separator)] if pd.notna(source_row[col]) else [None]
                        for col in columns_to_split
                    ]
                    
                    for combination in product(*split_values_lists):
                        new_row = {col: source_row[col] for col in columns_to_copy}
                        new_row.update(dict(zip(columns_to_split, combination)))
                        normalized_rows.append(new_row)
                
                # If the chunk generated data, append it to the target table
                if normalized_rows:
                    normalized_df = pd.DataFrame(normalized_rows)
                    
                    # Use to_sql with if_exists='append' to add the new rows
                    normalized_df.to_sql(
                        name=target_table_name,
                        con=engine,
                        schema=schema_name,
                        if_exists='append',
                        index=False
                    )
            
            logger.debug(f"Successfully populated {full_target_name}. Total source rows processed: {total_rows_processed}.")

        except Exception as e:
            # Using logger.exception provides a full stack trace for better debugging, logged at the ERROR level.
            logger.exception(f"Failed to process split for table '{source_table_name}'.")