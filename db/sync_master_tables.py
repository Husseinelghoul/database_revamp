import pandas as pd
import os
from sqlalchemy import VARCHAR, MetaData, Table, create_engine, inspect, text, NVARCHAR

from db.data_migrator import migrate_data
from utils.logger import setup_logger

logger = setup_logger()

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
    Refreshes master tables entirely from a CSV file.

    This version will:
    1.  **Delete All Data**: It completely clears the target table.
    2.  **Reset the ID Column**: It reseeds the identity counter so that new entries start from 1.
    3.  **Repopulate from CSV**: It inserts a clean, unique list of values from the CSV file.
    4.  **Manages Schema**: It still automatically alters VARCHAR and NVARCHAR columns to prevent future truncation errors.
    """
    master_df = pd.read_csv(csv_path)
    engine = create_engine(target_db_url)
    inspector = inspect(engine)

    with engine.begin() as conn:
        for table_name in master_df.columns:
            # Get a clean, unique list of values from the CSV.
            distinct_values = master_df[table_name].dropna().unique().tolist()

            if not distinct_values:
                logger.info(f"No values in CSV for table {table_name}. Clearing table only.")
                # You might want to just clear the table if the CSV column is empty.
                try:
                    safe_table_identifier = f'"{schema_name}"."{table_name}"'
                    conn.execute(text(f"DELETE FROM {safe_table_identifier};"))
                    logger.info(f"Cleared table {safe_table_identifier} as it had no corresponding values in the CSV.")
                except Exception as e:
                    logger.error(f"Failed to clear table {schema_name}.{table_name}: {e}")
                continue # Move to the next table
            
            try:
                # === 1. Inspect and Alter Schema (Unchanged) ===
                columns = inspector.get_columns(table_name, schema=schema_name)
                for col in columns:
                    is_string_type = isinstance(col['type'], (VARCHAR, NVARCHAR))
                    
                    if is_string_type and col['type'].length is not None and col['type'].length < 255:
                        new_type_name = col['type'].__class__.__name__
                        logger.info(
                            f"Altering column '{col['name']}' in table '{table_name}' "
                            f"from {new_type_name}({col['type'].length}) to {new_type_name}(255)."
                        )
                        alter_sql = text(
                            f'ALTER TABLE "{schema_name}"."{table_name}" '
                            f'ALTER COLUMN "{col["name"]}" {new_type_name}(255) NULL'
                        )
                        conn.execute(alter_sql)

                # === 2. Find Target Column ===
                identity_cols = {c["name"] for c in columns if c.get("autoincrement")}
                target_column = next((c["name"] for c in columns if c["name"] not in identity_cols), None)
                
                if not target_column:
                    raise ValueError(f"No non-identity column found in table '{table_name}' to insert into.")

                # === 3. Wipe, Reset, and Repopulate ===
                safe_table_identifier = f'"{schema_name}"."{table_name}"'
                safe_column_identifier = f'"{target_column}"'

                # Step 3a: Delete all existing data from the table.
                logger.warning(f"Deleting all data from {safe_table_identifier}.")
                conn.execute(text(f"DELETE FROM {safe_table_identifier};"))
                
                # Step 3b: Reseed the identity column to start from 1.
                # DBCC CHECKIDENT RESEED, 0 means the next entry will get an ID of 1.
                if identity_cols:
                    logger.warning(f"Resetting identity ID for {safe_table_identifier}.")
                    conn.execute(text(f"DBCC CHECKIDENT ('{safe_table_identifier}', RESEED, 0);"))
                
                # Step 3c: Insert the clean list of values from the CSV.
                for value in distinct_values:
                    insert_sql = text(f"INSERT INTO {safe_table_identifier} ({safe_column_identifier}) VALUES (:value);")
                    conn.execute(insert_sql, {"value": str(value).strip()})
                
                logger.info(f"Successfully refreshed and populated table {schema_name}.{table_name} with {len(distinct_values)} values.")

            except Exception as e:
                logger.error(f"Failed to process table {schema_name}.{table_name}: {e}")
                raise e # Stop execution on error

def merge_master_tables(target_db_url, schema_name, csv_folder_path="config/master_tables/"):
    """
    Merges new data from CSV files into their corresponding database tables.

    This function iterates through a folder of CSV files. For each file, it assumes
    the filename (without the .csv extension) matches a table name. It reads the
    CSV and compares its rows to the existing rows in the database table. Only the
    rows that do not already exist in the database are inserted.

    Key Assumptions:
    - The column names in each CSV file must exactly match the column names in the
      corresponding database table.
    - Existing data in the database will NOT be deleted or altered.

    Args:
        target_db_url (str): The connection string for the target database
                             (e.g., 'mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server').
        schema_name (str): The name of the database schema where the tables reside.
        csv_folder_path (str): The path to the folder containing the CSV files to process.
    """
    try:
        engine = create_engine(target_db_url)
        csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
    except FileNotFoundError:
        logger.error(f"Error: The folder at '{csv_folder_path}' was not found.")
        raise
    except ImportError:
        logger.error("Error: The necessary database driver (e.g., pyodbc) is not installed.")
        raise
        
    if not csv_files:
        logger.warning(f"No CSV files found in '{csv_folder_path}'. No tables were processed.")
        return

    logger.info(f"Found {len(csv_files)} CSV files to process. Starting merge operation.")

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        file_path = os.path.join(csv_folder_path, csv_file)
        
        try:
            with engine.connect() as conn:
                # 1. Read the existing data from the database table
                logger.info(f"Reading existing data from table: '{schema_name}.{table_name}'...")
                existing_data_df = pd.read_sql_table(table_name, conn, schema=schema_name)
                
                # 2. Read the new data from the CSV file
                new_data_df = pd.read_csv(file_path)

                if new_data_df.empty:
                    logger.info(f"CSV file '{csv_file}' is empty. Skipping.")
                    continue

                # 3. Identify rows in the CSV that are NOT in the database
                # We perform a left merge to find rows that are unique to the CSV data.
                merged_df = new_data_df.merge(
                    existing_data_df, 
                    how='left', 
                    indicator=True
                )
                
                rows_to_insert = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

                # 4. Insert only the new rows into the database
                if not rows_to_insert.empty:
                    logger.info(f"Found {len(rows_to_insert)} new row(s) in '{csv_file}'. Inserting into '{table_name}'...")
                    rows_to_insert.to_sql(
                        name=table_name,
                        con=conn,
                        schema=schema_name,
                        if_exists='append',
                        index=False
                    )
                    logger.info(f"Successfully inserted new data into '{schema_name}.{table_name}'.")
                else:
                    logger.info(f"No new data to insert for '{table_name}'. Database is already up-to-date.")

        except Exception as e:
            logger.error(f"Failed to process table '{schema_name}.{table_name}' from file '{csv_file}': {e}")
            # Continue to the next file instead of stopping the entire process
            continue