from sqlalchemy import create_engine, text
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def load_schema_changes(csv_path):
    """Helper function to load CSV changes."""
    return pd.read_csv(csv_path)


def change_data_types(target_db_url, schema_name, csv_path="config/data_quality_changes/data_types.csv"):
    """
    Change column data types based on the data_types.csv file.
    Each row should have: table_name, column_name, old_data_type, new_data_type
    """
    dtypes_df = load_schema_changes(csv_path)

    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in dtypes_df.iterrows():
            table = row['table_name']
            column = row['column_name']
            old_type = row['old_data_type']
            new_type = row['new_data_type']

            try:
                # Construct ALTER TABLE command
                alter_sql = f"""
                    ALTER TABLE {schema_name}.{table}
                    ALTER COLUMN {column} {new_type};
                """
                conn.execute(text(alter_sql))
                logger.debug(f"Changed type of {schema_name}.{table}.{column} from {old_type} to {new_type}")
            except Exception as e:
                logger.error(f"Failed to change type of {schema_name}.{table}.{column}: {e}")
                raise


def apply_constraints(target_db_url, schema_name, csv_path="config/data_quality_changes/constraints.csv"):
    """
    Apply constraints (MAX, MIN, UNIQUE) based on constraints.csv.
    Columns: table_name, column_name, constraint_type, value
    """
    constraints_df = load_schema_changes(csv_path)

    engine = create_engine(target_db_url)
    with engine.begin() as conn:
        for _, row in constraints_df.iterrows():
            table = row['table_name']
            column = row['column_name']
            constraint_type = row['constraint_type'].upper()
            value = row.get('value')  # May be NaN for UNIQUE

            try:
                if constraint_type == "MAX":
                    sql = f"""
                        ALTER TABLE {schema_name}.{table}
                        ADD CONSTRAINT CK_{table}_{column}_max CHECK ({column} <= {value});
                    """
                elif constraint_type == "MIN":
                    sql = f"""
                        ALTER TABLE {schema_name}.{table}
                        ADD CONSTRAINT CK_{table}_{column}_min CHECK ({column} >= {value});
                    """
                elif constraint_type == "UNIQUE":
                    sql = f"""
                        ALTER TABLE {schema_name}.{table}
                        ADD CONSTRAINT UQ_{table}_{column} UNIQUE ({column});
                    """
                else:
                    logger.warning(f"Unknown constraint type '{constraint_type}' for {schema_name}.{table}.{column}")
                    continue

                conn.execute(text(sql))
                logger.debug(f"Applied {constraint_type} constraint on {schema_name}.{table}.{column}")
            except Exception as e:
                logger.error(f"Failed to apply constraint on {schema_name}.{table}.{column}: {e}")
                raise