from sqlalchemy import create_engine, text

from utils.logger import setup_logger
from utils.utils import load_schema_changes

logger = setup_logger()

def add_primary_keys(target_db_url, schema_name, csv_path="config/data_integrity_changes/primary_keys.csv"):
    """Add primary keys specified in the CSV file."""
    primary_keys_df = load_schema_changes(csv_path)
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in primary_keys_df.iterrows():
            try:
                table_name = row["table_name"]
                column_name = row["column_name"]
                conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({column_name})"))
                logger.debug(f"Added primary key on column {column_name} for table {schema_name}.{table_name}")
            except Exception as e:
                logger.error(f"Failed to add primary key on column {column_name} for table {schema_name}.{table_name}: {e}")
                raise e

def add_unique_constraints(target_db_url, schema_name, csv_path="config/data_integrity_changes/unique_constraints.csv"):
    """Add unique constraints specified in the CSV file."""
    unique_constraints_df = load_schema_changes(csv_path)
    if unique_constraints_df.empty():
        logger.info("No unique constraints")
    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in unique_constraints_df.iterrows():
            try:
                table_name = row["table_name"]
                column_names = row["column_names"].replace("-", ", ")  # Replace hyphen with comma for SQL syntax
                conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD UNIQUE ({column_names})"))
                logger.debug(f"Added unique constraint on columns {column_names} for table {schema_name}.{table_name}")
            except Exception as e:
                logger.error(f"Failed to add unique constraint on columns {column_names} for table {schema_name}.{table_name}: {e}")
                raise e