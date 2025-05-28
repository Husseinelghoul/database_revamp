import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger

logger = setup_logger()

def populate_master_tables(target_db_url, schema_name, csv_path="config/master_table_values/master_values.csv"):
    """
    Populate master tables with distinct values from a CSV file, dropping existing rows first.
    Automatically identifies the second column name (excluding 'id') for insertion.
    
    The CSV file should have the following format:
    - Table names as column headers.
    - Distinct values listed below each table name.
    - Skip tables with only one row and the value is 'empty'.
    """
    # Load the CSV file
    master_df = pd.read_csv(csv_path)
    
    # Connect to the MSSQL database
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for table_name in master_df.columns:
            distinct_values = master_df[table_name].dropna().tolist()
            
            # Skip the table if it contains only one row and the value is 'empty'
            if len(distinct_values) == 1 and distinct_values[0].strip().lower() == "empty":
                logger.debug(f"Skipping table {table_name} as it contains only 'empty'.")
                continue
            
            try:
                second_column = table_name.replace("master_","")
                
                # Delete all existing rows in the table
                conn.execute(text(f"DELETE FROM {schema_name}.{table_name};"))
                logger.debug(f"Deleted all rows from {schema_name}.{table_name}.")
                
                # Reset the ID column to start from 1
                conn.execute(text(f"""
                    DBCC CHECKIDENT ('{schema_name}.{table_name}', RESEED, 0);
                """))
                logger.debug(f"Reset ID column for {schema_name}.{table_name} to start from 1.")
                
                # Insert each distinct value into the master table
                for value in distinct_values:
                    conn.execute(text(f"""
                        INSERT INTO {schema_name}.{table_name} ({second_column})
                        VALUES (:value);
                    """), {"value": value.strip()})
                
                logger.debug(f"Inserted values into {schema_name}.{table_name}: {distinct_values}")
            except Exception as e:
                logger.error(f"Failed to populate table {schema_name}.{table_name}: {e}")
                raise e