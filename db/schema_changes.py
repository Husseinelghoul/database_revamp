from sqlalchemy import create_engine, text

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
    Create new tables by splitting rows based on separator values in specified columns.
    
    The CSV file should have the following columns:
    - source_table_name: Name of the source table
    - columns_to_copy: Semicolon-separated list of columns to copy as-is
    - columns_to_split: Semicolon-separated list of columns that contain values to split
    - separator: The separator character used in the split columns
    - target_table_name: Name of the new table to be created
    """
    # Load the schema changes from the CSV file
    splits_df = load_schema_changes(csv_path)
    
    # Connect to the database
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        for _, row in splits_df.iterrows():
            source_table_name = row['source_table_name']
            columns_to_copy = [col.strip() for col in row['columns_to_copy'].split(';')]
            columns_to_split = [col.strip() for col in row['columns_to_split'].split(';')]
            separator = row['separator']
            target_table_name = row['target_table_name']
            
            try:
                # Get the structure of the source table
                all_columns = columns_to_copy + columns_to_split
                
                # First, get column definitions for creating the target table
                column_definitions = []
                for col in all_columns:
                    result = conn.execute(text("""
                        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name AND COLUMN_NAME = :column_name;
                    """), {"schema_name": schema_name, "table_name": source_table_name, "column_name": col})
                    
                    column_info = result.fetchone()
                    if not column_info:
                        raise ValueError(f"Column {col} not found in {schema_name}.{source_table_name}.")
                    
                    # Extract the column details
                    data_type = column_info[0]
                    char_max_length = column_info[1]
                    numeric_precision = column_info[2]
                    numeric_scale = column_info[3]
                    
                    # Construct the column definition
                    if data_type.upper() in ["VARCHAR", "NVARCHAR", "CHAR", "NCHAR"]:
                        if char_max_length == -1:  # Handle MAX for VARCHAR(MAX), NVARCHAR(MAX)
                            column_definition = f"{col} {data_type}(MAX)"
                        else:
                            column_definition = f"{col} {data_type}({char_max_length})"
                    elif data_type in ["DECIMAL", "NUMERIC"]:
                        column_definition = f"{col} {data_type}({numeric_precision}, {numeric_scale})"
                    else:
                        column_definition = f"{col} {data_type}"
                    
                    column_definitions.append(column_definition)
                
                # Create the target table
                create_table_sql = f"""
                    CREATE TABLE {schema_name}.{target_table_name} (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        {', '.join(column_definitions)}
                    );
                """
                conn.execute(text(create_table_sql))
                logger.debug(f"Created target table {schema_name}.{target_table_name}")
                
                # Fetch data from source table
                select_columns = ', '.join(all_columns)
                source_data = conn.execute(text(f"""
                    SELECT {select_columns}
                    FROM {schema_name}.{source_table_name};
                """))
                
                # Process each row from the source table
                for source_row in source_data:
                    # Convert row to dictionary for easier access
                    row_dict = dict(zip(all_columns, source_row))
                    
                    # Get values to copy (these stay the same for all new rows)
                    copy_values = {col: row_dict[col] for col in columns_to_copy}
                    
                    # Get values to split and create all combinations
                    split_values_lists = []
                    for col in columns_to_split:
                        if row_dict[col] is not None:
                            # Split the value by separator and strip whitespace
                            split_vals = [val.strip() for val in str(row_dict[col]).split(separator)]
                            split_values_lists.append(split_vals)
                        else:
                            # Handle NULL values
                            split_values_lists.append([None])
                    
                    # Create cartesian product of all split values
                    for combination in product(*split_values_lists):
                        # Create new row data
                        new_row_data = copy_values.copy()
                        
                        # Add split column values
                        for i, col in enumerate(columns_to_split):
                            new_row_data[col] = combination[i]
                        
                        # Prepare INSERT statement
                        columns_list = list(new_row_data.keys())
                        values_list = list(new_row_data.values())
                        
                        placeholders = ', '.join([':' + col for col in columns_list])
                        insert_sql = f"""
                            INSERT INTO {schema_name}.{target_table_name} ({', '.join(columns_list)})
                            VALUES ({placeholders});
                        """
                        
                        # Create parameter dictionary
                        params = {col: val for col, val in zip(columns_list, values_list)}
                        
                        conn.execute(text(insert_sql), params)
                
                logger.debug(f"Successfully split table {source_table_name} into {target_table_name}")
                
            except Exception as e:
                logger.error(f"Failed to split table {source_table_name} into {target_table_name}: {e}")
                # Optionally, drop the target table if it was created but population failed
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{target_table_name};"))
                except:
                    pass
                raise