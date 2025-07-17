import pandas as pd
import os
from sqlalchemy import VARCHAR, MetaData, Table, create_engine, inspect, text, NVARCHAR

from db.data_migrator import migrate_data
from utils.logger import setup_logger

logger = setup_logger()
from sqlalchemy import inspect, text
from sqlalchemy.types import VARCHAR, NVARCHAR
# other necessary imports (pandas, os, sqlalchemy.create_engine, logging)
import pandas as pd
import os
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)


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
        master_table_names = [name for name in all_target_tables if "master" in name.lower()]

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
    target_inspector = inspect(target_engine)
    source_meta = MetaData()

    try:
        # === Step 1: Get the list of master tables from the source ===
        all_source_tables = source_inspector.get_table_names(schema=source_schema)
        master_table_names = [name for name in all_source_tables if "master" in name.lower()]
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
                # === THE FIX: Resize small text columns before doing anything else ===
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


def merge_master_tables(target_db_url, schema_name, csv_folder_path="config/master_tables/"):
    """
    Merges new data from CSV files into their corresponding database tables.
    """
    try:
        engine = create_engine(target_db_url)
        inspector = inspect(engine) # Create inspector once
        csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
    except FileNotFoundError:
        logger.error(f"Error: The folder at '{csv_folder_path}' was not found.")
        raise
        
    if not csv_files:
        logger.warning(f"No CSV files found in '{csv_folder_path}'. No tables were processed.")
        return

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        file_path = os.path.join(csv_folder_path, csv_file)
        
        try:
            with engine.connect() as conn:
                # 1. Read existing data
                existing_data_df = pd.read_sql_table(table_name, conn, schema=schema_name)
                
                # 2. Read new data
                new_data_df = pd.read_csv(file_path)

                if new_data_df.empty:
                    continue

                # === THE FIX: Align data types before merging ===
                db_dtypes = existing_data_df.dtypes
                for col in new_data_df.columns.intersection(db_dtypes.index):
                    target_dtype = db_dtypes[col]
                    if new_data_df[col].dtype != target_dtype:
                        try:
                            # Use pd.to_numeric for robust conversion of number types
                            if pd.api.types.is_numeric_dtype(target_dtype):
                                # 'coerce' will turn non-numeric values into NaN
                                new_data_df[col] = pd.to_numeric(new_data_df[col], errors='coerce')
                            
                            # Cast to the exact target type from the database
                            new_data_df = new_data_df.astype({col: target_dtype})
                            logger.debug(f"Converted column '{col}' to {target_dtype} for {csv_file}")
                        except Exception as e:
                            logger.warning(f"Could not cast column '{col}' in {csv_file} to {target_dtype}. Error: {e}")
                # === END OF FIX ===

                # 3. Identify and insert new rows
                merged_df = new_data_df.merge(existing_data_df, how='left', indicator=True)
                rows_to_insert = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

                if not rows_to_insert.empty:
                    rows_to_insert.to_sql(
                        name=table_name,
                        con=conn,
                        schema=schema_name,
                        if_exists='append',
                        index=False
                    )
                    logger.debug(f"Successfully inserted {len(rows_to_insert)} new rows into '{schema_name}.{table_name}'.")

        except Exception as e:
            logger.error(f"Failed to process table '{schema_name}.{table_name}' from file '{csv_file}': {e}")
            continue

def merge_master_tables(target_db_url, schema_name, csv_folder_path="config/master_tables/"):
    """
    Merges new data from CSV files into their corresponding database tables.
    """
    try:
        engine = create_engine(target_db_url)
        inspector = inspect(engine)
        csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
    except FileNotFoundError:
        logger.error(f"Error: The folder at '{csv_folder_path}' was not found.")
        raise
        
    if not csv_files:
        logger.warning(f"No CSV files found in '{csv_folder_path}'. No tables were processed.")
        return

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        file_path = os.path.join(csv_folder_path, csv_file)
        
        try:
            with engine.connect() as conn:
                # 1. Read existing data and schema info
                existing_data_df = pd.read_sql_table(table_name, conn, schema=schema_name)
                db_dtypes = existing_data_df.dtypes
                
                # 2. Read new data
                new_data_df = pd.read_csv(file_path)

                if new_data_df.empty:
                    logger.info(f"CSV file '{csv_file}' is empty. Skipping.")
                    continue

                # 3. Align data types
                for col in new_data_df.columns.intersection(db_dtypes.index):
                    target_dtype = db_dtypes[col]
                    if new_data_df[col].dtype != target_dtype:
                        try:
                            if pd.api.types.is_numeric_dtype(target_dtype):
                                new_data_df[col] = pd.to_numeric(new_data_df[col], errors='coerce')
                            new_data_df = new_data_df.astype({col: target_dtype})
                        except Exception as e:
                            logger.warning(f"Could not cast column '{col}' in {csv_file} to {target_dtype}. Error: {e}")

                # 4. Identify new rows
                merged_df = new_data_df.merge(existing_data_df, how='left', indicator=True)
                rows_to_insert = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

                if not rows_to_insert.empty:
                    # === THE FIX: Remove identity column before insert ===
                    # Dynamically find the table's identity/autoincrementing column(s).
                    identity_cols = [
                        col['name'] for col in inspector.get_columns(table_name, schema=schema_name)
                        if col.get('autoincrement', False)
                    ]
                    
                    # Drop the identity column(s) from the DataFrame.
                    # This lets the database generate the ID automatically.
                    final_rows_to_insert = rows_to_insert.drop(columns=identity_cols, errors='ignore')
                    # === END OF FIX ===

                    final_rows_to_insert.to_sql(
                        name=table_name,
                        con=conn,
                        schema=schema_name,
                        if_exists='append',
                        index=False
                    )
                    logger.info(f"Successfully inserted {len(final_rows_to_insert)} new rows into '{schema_name}.{table_name}'.")
                else:
                    logger.info(f"No new rows to insert for table '{schema_name}.{table_name}'.")

        except Exception as e:
            logger.error(f"Failed to process table '{schema_name}.{table_name}' from file '{csv_file}': {e}")
            continue