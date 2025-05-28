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