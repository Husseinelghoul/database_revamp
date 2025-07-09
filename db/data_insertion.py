import pandas as pd
from sqlalchemy import VARCHAR, create_engine, inspect, text

from utils.logger import setup_logger

logger = setup_logger()

def populate_master_tables(target_db_url, schema_name, csv_path="config/master_table_values/master_values.csv"):
    """
    Populates master tables from a CSV.
    
    This version automatically alters any VARCHAR columns smaller than 255 to VARCHAR(255)
    to prevent truncation errors. It also includes security and robustness improvements.
    """
    master_df = pd.read_csv(csv_path)
    engine = create_engine(target_db_url)
    inspector = inspect(engine)

    with engine.begin() as conn:
        for table_name in master_df.columns:
            distinct_values = master_df[table_name].dropna().tolist()

            if len(distinct_values) == 1 and distinct_values[0].strip().lower() == "empty":
                logger.debug(f"Skipping table {table_name} as it contains only 'empty'.")
                continue
            
            try:
                # === 1. Inspect and Alter Schema (Your Requested Change) ===
                columns = inspector.get_columns(table_name, schema=schema_name)
                for col in columns:
                    # Check if the column type is a string type with a defined length
                    if isinstance(col['type'], VARCHAR) and col['type'].length is not None:
                        if col['type'].length < 255:
                            logger.info(f"Altering column '{col['name']}' in table '{table_name}' from VARCHAR({col['type'].length}) to VARCHAR(255).")
                            # Safely quote identifiers to prevent SQL injection
                            alter_sql = text(f'ALTER TABLE "{schema_name}"."{table_name}" ALTER COLUMN "{col["name"]}" VARCHAR(255)')
                            conn.execute(alter_sql)

                # === 2. Robustly Find the Target Column (Improvement) ===
                # This is safer than replacing 'master_' in the table name
                identity_cols = {c["name"] for c in columns if c.get("autoincrement")}
                # Find the first column that is not an identity column
                target_column = next((c["name"] for c in columns if c["name"] not in identity_cols), None)
                
                if not target_column:
                    raise ValueError(f"No non-identity column found in table {table_name} to insert into.")

                # === 3. Safely Execute SQL (Security Fix) ===
                # Use quoted identifiers to prevent SQL injection vulnerabilities
                safe_table_identifier = f'"{schema_name}"."{table_name}"'
                safe_column_identifier = f'"{target_column}"'

                # Delete existing rows
                conn.execute(text(f"DELETE FROM {safe_table_identifier};"))
                
                # Reset the ID column if the table has one
                if identity_cols:
                    conn.execute(text(f"DBCC CHECKIDENT ('{safe_table_identifier}', RESEED, 0);"))
                
                # Insert new values
                for value in distinct_values:
                    insert_sql = text(f"INSERT INTO {safe_table_identifier} ({safe_column_identifier}) VALUES (:value);")
                    conn.execute(insert_sql, {"value": value.strip()})
                
                logger.debug(f"Successfully populated table {schema_name}.{table_name}.")
            except Exception as e:
                logger.error(f"Failed to populate table {schema_name}.{table_name}: {e}")
                raise e