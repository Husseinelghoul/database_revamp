import json
from sqlalchemy.exc import SQLAlchemyError

import os
import pandas as pd
from sqlalchemy import create_engine, text
from utils.logger import setup_logger

logger = setup_logger()

# --- HELPER FUNCTIONS ---


def parse_json_cell(cell_value):
    """Safely parse a JSON cell and extract file_path and phase."""
    try:
        data = json.loads(cell_value)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [{'file_path': str(item.get('file_path', '')).strip('"'), 'phase': str(item.get('phase', '')).strip()} for item in data]
    except (json.JSONDecodeError, TypeError):
        pass
    return []

def parse_simple_json_array(cell_value):
    """
    Safely parse a JSON cell expecting a simple list of strings (file paths).
    Handles non-string values and empty lists gracefully.
    """
    try:
        data = json.loads(cell_value)
        if isinstance(data, list):
            # Ensure all items in the list are non-empty strings
            return [str(item) for item in data if item]
    except (json.JSONDecodeError, TypeError):
        pass
    return []

def get_mime_type(path):
    """Helper to safely extract a file extension from a path."""
    if not isinstance(path, str) or '.' not in path:
        return None
    return os.path.splitext(path)[1].lstrip('.').lower()


# --- NEW DYNAMIC FUNCTION (REVISED) ---

def create_media_lookup(target_db_url, schema_name, config_csv_path="config/schema_changes/media_lookup.csv"):
    """
    Creates dedicated media lookup tables based on a configuration file.

    For each row in the configuration CSV, this function will:
    1. Create a new, separate target table.
    2. Define a schema for it with a specific foreign key back to the source table.
    3. Extract, transform, and load media file paths from the source into the new target table.

    Args:
        target_db_url (str): The database connection URL.
        schema_name (str): The name of the database schema.
        config_csv_path (str): Path to the CSV configuration file. The file must contain:
            - source_table_name: Name of the table with media columns.
            - source_primary_key: The primary key column of the source table.
            - source_media_columns: A semicolon-separated list of media columns.
            - target_table_name: The name of the new lookup table to be created.
            - foreign_key_column: The name for the foreign key column in the target table.
    """
    engine = create_engine(target_db_url)
    
    try:
        logger.info(f"Reading media lookup configuration from: {config_csv_path}")
        config_df = pd.read_csv(config_csv_path)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at: {config_csv_path}")
        raise

    # Iterate through each configuration row to create a separate table for each
    for index, config_row in config_df.iterrows():
        try:
            # --- 1. Read Configuration for the Current Task ---
            source_table = config_row['source_table_name']
            source_pk = config_row['source_primary_key']
            media_cols = config_row['source_media_columns'].split(';')
            target_table = config_row['target_table_name']
            target_fk_col = config_row['foreign_key_column']
            
            full_source_name = f'"{schema_name}"."{source_table}"'
            full_target_name = f'"{schema_name}"."{target_table}"'
            
            logger.info(f"--- Starting task: {source_table} -> {target_table} ---")

            # --- 2. Create the Dynamic Target Table ---
            with engine.begin() as conn:
                logger.debug(f"Dropping and creating new target table: {full_target_name}")
                conn.execute(text(f'DROP TABLE IF EXISTS {full_target_name};'))
                
                # Dynamically create the table with the specified foreign key
                create_sql = f"""
                    CREATE TABLE {full_target_name} (
                        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        {target_fk_col} INT NOT NULL,
                        media_type NVARCHAR(255) NOT NULL,
                        file_path NVARCHAR(MAX) NOT NULL,
                        mime_type NVARCHAR(100) NULL,
                        CONSTRAINT fk_{target_table}_{source_table} 
                            FOREIGN KEY ({target_fk_col}) 
                            REFERENCES {full_source_name}({source_pk})
                    );
                """
                conn.execute(text(create_sql))

            # --- 3. Load and Process Source Data ---
            logger.debug(f"Loading data from source table: {full_source_name}")
            select_cols = f'"{source_pk}", ' + ', '.join([f'"{col}"' for col in media_cols])
            source_df = pd.read_sql(f'SELECT {select_cols} FROM {full_source_name}', engine)
            
            # Unpivot media columns into a long format
            melted_df = source_df.melt(
                id_vars=[source_pk], 
                value_vars=media_cols, 
                var_name='media_type', 
                value_name='cell_value'
            )

            # --- 4. Clean, Parse, and Explode Data ---
            melted_df.dropna(subset=['cell_value'], inplace=True)
            melted_df = melted_df[melted_df['cell_value'].astype(str).str.strip().ne('[]')]
            
            if melted_df.empty:
                logger.debug(f"No media data found for this task. The table {full_target_name} will be empty.")
                continue

            melted_df['file_path'] = melted_df['cell_value'].apply(parse_simple_json_array)
            output_df = melted_df.explode('file_path').dropna(subset=['file_path'])
            
            # --- 5. Prepare Final DataFrame for Insertion ---
            output_df.rename(columns={source_pk: target_fk_col}, inplace=True)
            output_df['file_path'] = output_df['file_path'].astype(str).str.strip().str.strip('"')
            output_df['mime_type'] = output_df['file_path'].apply(get_mime_type)

            final_cols = [target_fk_col, 'media_type', 'file_path', 'mime_type']
            output_df = output_df[final_cols].copy()
            output_df.dropna(subset=['file_path', target_fk_col], inplace=True)
            output_df = output_df[output_df['file_path'] != '']
            
            if output_df.empty:
                logger.debug(f"No valid file paths found after parsing for table {full_target_name}.")
                continue

            # --- 6. Insert Data into the New Table ---
            logger.info(f"Inserting {len(output_df)} records into {full_target_name}...")
            output_df.to_sql(
                name=target_table, 
                con=engine, 
                schema=schema_name, 
                if_exists='append', 
                index=False, 
                chunksize=500
            )
            logger.info(f"--- Successfully completed task: {source_table} -> {target_table} ---")

        except SQLAlchemyError as e:
            logger.error(f"A database error occurred while processing row {index} ({source_table}): {e}", exc_info=True)
            # Continue to the next row in the CSV
            continue
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing row {index} ({source_table}): {e}", exc_info=True)
            # Continue to the next row in the CSV
            continue
            
    logger.info("All tasks from the configuration file have been processed.")


def create_lookup_project_to_media(target_db_url, schema_name):
    """
    Final Clean Version:
    1. Skips NULL/empty values from the source.
    2. Fixes the FutureWarning by filtering empty dataframes before concatenation.
    3. Uses optimized, vectorized operations.
    """
    MEDIA_COLUMNS = [
        'decree_files', 'plot_plan_files', 'static_images_files',
        'dynamic_images_files', 'additional_documents_files',
        'progress_videos', 'project_management_files'
    ]
    engine = create_engine(target_db_url)
    full_target_name = f'"{schema_name}"."lookup_project_to_media"'
    full_source_name = f'"{schema_name}"."project_summary"'
    full_lookup_name = f'"{schema_name}"."lookup_project_to_phase"'

    try:
        # Step 1: Prepare the destination table
        with engine.begin() as conn:
            logger.debug(f"Setting up fresh target table {full_target_name}...")
            conn.execute(text(f'DROP TABLE IF EXISTS {full_target_name};'))
            media_type_check_list = ", ".join([f"'{col}'" for col in MEDIA_COLUMNS])
            create_sql = f"""
                CREATE TABLE {full_target_name} (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    project_summary_id INT NOT NULL,
                    lookup_project_to_phase_id INT NULL,
                    media_type NVARCHAR(255) NOT NULL,
                    file_path NVARCHAR(MAX) NOT NULL,
                    mime_type NVARCHAR(100) NULL,
                    CONSTRAINT fk_media_to_summary_final FOREIGN KEY (project_summary_id) REFERENCES {full_source_name}(id),
                    CONSTRAINT fk_media_to_phase_final FOREIGN KEY (lookup_project_to_phase_id) REFERENCES {full_lookup_name}(id),
                    CONSTRAINT chk_media_type_final CHECK (media_type IN ({media_type_check_list}))
                );
            """
            conn.execute(text(create_sql))

        # Step 2: Load all necessary data
        logger.debug("Loading source and lookup data into memory...")
        source_df = pd.read_sql(f'SELECT id, project_name, period, {", ".join(MEDIA_COLUMNS)} FROM {full_source_name}', engine)
        lookup_df = pd.read_sql(f'SELECT id, project_name, period, phase FROM {full_lookup_name}', engine)
        lookup_df.rename(columns={'id': 'lookup_project_to_phase_id'}, inplace=True)

        # Step 3: Unpivot the source data into a long format
        id_vars = ['id', 'project_name', 'period']
        melted_df = source_df.melt(id_vars=id_vars, value_vars=MEDIA_COLUMNS, var_name='media_type', value_name='cell_value')
        melted_df.rename(columns={'id': 'project_summary_id'}, inplace=True)

        # Step 4: Filter out empty/NULL rows
        melted_df.dropna(subset=['cell_value'], inplace=True)
        melted_df['cell_value_str'] = melted_df['cell_value'].astype(str).str.strip()
        melted_df = melted_df[melted_df['cell_value_str'] != '[]']
        melted_df = melted_df[melted_df['cell_value_str'] != '']

        if melted_df.empty:
            logger.warning("No valid media file data found to process.")
            return

        # Step 5: Separate data by format
        is_json_format = melted_df['cell_value_str'].str.startswith('[{"')
        df_json = melted_df[is_json_format].copy()
        df_simple = melted_df[~is_json_format].copy()
        
        final_dfs = []

        # Step 6: Process Format 2 (JSON with phase)
        if not df_json.empty:
            logger.debug(f"Processing {len(df_json)} JSON format rows...")
            df_json['parsed'] = df_json['cell_value'].apply(parse_json_cell)
            df_json = df_json.explode('parsed').dropna(subset=['parsed'])
            df_json.reset_index(drop=True, inplace=True)

            parsed_df = pd.json_normalize(df_json['parsed'])
            df_json = pd.concat([df_json.drop(columns=['parsed', 'cell_value', 'cell_value_str']), parsed_df], axis=1)

            for key in ['project_name', 'period', 'phase']:
                df_json[key] = df_json[key].astype(str).str.strip()
                lookup_df[key] = lookup_df[key].astype(str).str.strip()
            
            merged_json = pd.merge(df_json, lookup_df, on=['project_name', 'period', 'phase'], how='left')
            final_dfs.append(merged_json)

        # Step 7: Process Format 1 (Simple array)
        if not df_simple.empty:
            logger.debug(f"Processing {len(df_simple)} simple format rows...")
            df_simple['file_path'] = df_simple['cell_value'].astype(str).str.strip('[]"').str.split('","')
            df_simple = df_simple.explode('file_path')
            df_simple['lookup_project_to_phase_id'] = None
            final_dfs.append(df_simple)

        # === FIX: Filter out empty dataframes before concatenation to avoid the warning ===
        final_dfs = [df for df in final_dfs if not df.empty]
        if not final_dfs:
            logger.warning("No records were generated after processing.")
            return

        # Step 8: Combine, clean, and insert
        output_df = pd.concat(final_dfs, ignore_index=True)
        output_df['file_path'] = output_df['file_path'].astype(str).str.strip().str.strip('"')
        output_df['mime_type'] = output_df['file_path'].apply(get_mime_type)
        
        final_cols = ['project_summary_id', 'lookup_project_to_phase_id', 'media_type', 'file_path', 'mime_type']
        output_df = output_df[final_cols]
        output_df.dropna(subset=['file_path'], inplace=True)
        output_df = output_df[output_df['file_path'] != '']

        logger.info(f"Processed data. Inserting {len(output_df)} records into the database...")
        output_df.to_sql(name='lookup_project_to_media', con=engine, schema=schema_name, if_exists='append', index=False, chunksize=400)
        
        logger.debug("Successfully created and populated the media lookup table.")

    except Exception as e:
        logger.error(f"A critical error occurred during the media lookup creation: {e}", exc_info=True)
        raise