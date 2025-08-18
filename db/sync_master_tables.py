import os

import pandas as pd
from sqlalchemy import (NVARCHAR, VARCHAR, MetaData, Table, create_engine,
                        inspect, text)
from sqlalchemy.types import NVARCHAR, VARCHAR

from db.data_migrator import migrate_data
from utils.logger import setup_logger

logger = setup_logger()



def _resize_text_columns(conn, inspector, schema_name, table_name):
    """
    Helper function to inspect and resize VARCHAR/NVARCHAR columns.

    Checks for text columns smaller than 50 characters and alters them to 255.
    """
    columns = inspector.get_columns(table_name, schema=schema_name)
    for col in columns:
        is_string_type = isinstance(col['type'], (VARCHAR, NVARCHAR))
        
        # Condition: Is it a string type with a defined length less than 50?
        if is_string_type and col['type'].length is not None and col['type'].length < 255:
            
            # Get the base type name ('VARCHAR' or 'NVARCHAR') to preserve it
            new_type_name = col['type'].__class__.__name__
            
            logger.debug(
                f"Altering column '{col['name']}' in table '{table_name}' "
                f"from {new_type_name}({col['type'].length}) to {new_type_name}(255)."
            )
            alter_sql = text(
                f'ALTER TABLE "{schema_name}"."{table_name}" '
                f'ALTER COLUMN "{col["name"]}" {new_type_name}(255) NULL'
            )
            conn.execute(alter_sql)

def drop_master_tables(target_db_url, target_schema):
    """
    Finds and drops all tables containing 'master' in their name
    from the target schema.
    """
    logger.debug(f"--- WARNING: Preparing to drop all master tables from schema: {target_schema} ---")
    engine = create_engine(target_db_url)
    inspector = inspect(engine)

    try:
        all_target_tables = inspector.get_table_names(schema=target_schema)
        master_table_names = [name for name in all_target_tables if "master" in name.lower()
                              and name.lower() != 'master_project_to_project_phase']
        

        if not master_table_names:
            logger.debug("No master tables found in target to drop.")
            return

        logger.warning(f"Found {len(master_table_names)} master tables to drop: {master_table_names}")
        with engine.begin() as conn:
            # It's often necessary to drop foreign keys first
            # For simplicity here, we will drop tables directly but log errors if they fail
            for table_name in reversed(master_table_names): # Drop in reverse to help with dependencies
                safe_identifier = f'"{target_schema}"."{table_name}"'
                try:
                    conn.execute(text(f"DROP TABLE {safe_identifier};"))
                    logger.debug(f"Successfully dropped table: {safe_identifier}")
                except Exception as e:
                    # This error often happens if the table is referenced by a foreign key
                    logger.error(f"Could not drop table {safe_identifier}. Error: {e}")
                    # In a full production script, you would handle this more robustly
                    # by dropping foreign key constraints first.

        logger.debug("--- Master table drop process complete. ---")

    except Exception as e:
        logger.error(f"An error occurred during the drop process: {e}")
        raise e
    finally:
        engine.dispose()

def streamlined_sync_master_tables(source_db_url, target_db_url, source_schema, target_schema):
    """
    A more efficient and robust function to sync all master tables and their data.
    This function replaces the need for separate read_schema and write_schema functions.
    """
    logger.debug(f"Starting streamlined sync of master tables from '{source_schema}' to '{target_schema}'...")
    source_engine = create_engine(source_db_url)
    target_engine = create_engine(target_db_url)
    source_inspector = inspect(source_engine)
    source_meta = MetaData()

    try:
        # === Step 1: Get the list of master tables from the source ===
        all_source_tables = source_inspector.get_table_names(schema=source_schema)
        master_table_names = [name for name in all_source_tables if "master" in name.lower() 
                              and name.lower() != 'master_project_to_project_phase']
        logger.debug(f"Found {len(master_table_names)} master tables to sync.")

        # === Step 2: Reflect and create the exact schema in the target ===
        drop_master_tables(target_db_url, target_schema)
        for table_name in master_table_names:
            # Use SQLAlchemy's reflection to automatically get the table's full structure
            table = Table(table_name, source_meta, autoload_with=source_engine, schema=source_schema)
            
            # Point the table to the new target schema
            table.schema = target_schema
            
            # Create the table in the target database
            table.create(bind=target_engine)
            logger.debug(f"Successfully created schema for {target_schema}.{table_name}")

        logger.debug("Schema synchronization complete.")

        # === Step 3: Migrate the data using our existing robust function ===
        # The migrate_data function needs a dictionary with table names as keys.
        master_schema_dict = {name: {} for name in master_table_names}

        logger.debug("Starting data migration for master tables...")
        migrate_data(
            source_db_url,
            target_db_url,
            master_schema_dict, # Pass the filtered list of tables
            source_schema,
            target_schema
        )
        logger.debug("Master table data migration complete.")

    except Exception as e:
        logger.error(f"An error occurred during master table sync: {e}")
        raise e
    finally:
        source_engine.dispose()
        target_engine.dispose()

def populate_master_tables(target_db_url, schema_name, csv_path="config/master_table_values/master_values.csv"):
    """
    Refreshes master tables by combining existing data with new data from a CSV,
    then rebuilds the table with a clean, sequential identity column.
    """
    master_df = pd.read_csv(csv_path)
    engine = create_engine(target_db_url)
    inspector = inspect(engine)

    with engine.begin() as conn:
        for table_name in master_df.columns:
            try:
                _resize_text_columns(conn, inspector, schema_name, table_name)

                # === Part 1: Gather and Combine All Data ===
                csv_values = {val.strip() for val in master_df[table_name].dropna()}
                columns = inspector.get_columns(table_name, schema=schema_name)
                identity_cols = {c["name"] for c in columns if c.get("autoincrement")}
                target_column = next((c["name"] for c in columns if c["name"] not in identity_cols), None)
                
                if not target_column:
                    raise ValueError(f"No non-identity column found in '{table_name}'.")

                safe_table_identifier = f'"{schema_name}"."{table_name}"'
                safe_column_identifier = f'"{target_column}"'

                select_sql = text(f"SELECT {safe_column_identifier} FROM {safe_table_identifier};")
                result = conn.execute(select_sql)
                existing_values = {row[0] for row in result if row[0] is not None}
                
                final_values = sorted(list(csv_values.union(existing_values)))

                # === Part 2: Wipe the Table and Reset the ID ===
                conn.execute(text(f"DELETE FROM {safe_table_identifier};"))
                if identity_cols:
                    conn.execute(text(f"DBCC CHECKIDENT ('{safe_table_identifier}', RESEED, 0);"))

                # === Part 3: Repopulate the Table ===
                for value in final_values:
                    insert_sql = text(f"INSERT INTO {safe_table_identifier} ({safe_column_identifier}) VALUES (:value);")
                    conn.execute(insert_sql, {"value": value})
                
                logger.debug(f"Successfully rebuilt {safe_table_identifier} with {len(final_values)} total values.")

            except Exception as e:
                logger.error(f"Failed to process table {schema_name}.{table_name}: {e}")
                raise e

