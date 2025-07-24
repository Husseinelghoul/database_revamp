import json
import os

import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger

logger = setup_logger()


def get_mime_type(path):
    """Helper to safely extract a file extension from a path."""
    if not isinstance(path, str) or '.' not in path:
        return None
    return os.path.splitext(path)[1].lstrip('.').lower()

def parse_json_cell_with_phase(cell_value):
    """
    Safely parse a JSON cell and extract both 'file_path' and 'phase'.
    Used by the original create_lookup_project_to_media function.
    """
    try:
        data = json.loads(cell_value)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [{'file_path': str(item.get('file_path', '')).strip('"'), 'phase': str(item.get('phase', '')).strip()} for item in data]
    except (json.JSONDecodeError, TypeError):
        pass
    return []

def parse_flexible_media_json(cell_value):
    """
    Safely parses a JSON cell that can contain either a simple array of strings
    OR an array of objects with a 'file_path' key, returning a clean list of paths.
    """
    if not cell_value or not isinstance(cell_value, str):
        return []
    try:
        data = json.loads(cell_value)
        if not isinstance(data, list) or not data:
            return []

        first_element = data[0]

        if isinstance(first_element, dict) and 'file_path' in first_element:
            # Handles [{"file_path": "path1.pdf"}, {"file_path": "path2.pdf"}]
            return [str(item.get('file_path', '')) for item in data if item.get('file_path')]
        elif isinstance(first_element, str):
            # Handles ["path1.pdf", "path2.pdf"]
            return [str(item) for item in data if item]
        else:
            return []
    except (json.JSONDecodeError, TypeError):
        return []

# --- GENERIC, CONFIG-DRIVEN FUNCTION (FULLY FIXED) ---

def create_media_lookup(target_db_url, schema_name, config_csv_path="config/schema_changes/media_lookup.csv"):
    """
    Creates dedicated media lookup tables based on a configuration file.
    This version is fixed to handle multiple JSON formats correctly.
    """
    engine = create_engine(target_db_url)
    try:
        config_df = pd.read_csv(config_csv_path)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at: {config_csv_path}")
        raise

    for index, config_row in config_df.iterrows():
        try:
            source_table = config_row['source_table_name']
            source_pk = config_row['source_primary_key']
            media_cols = config_row['source_media_columns'].split(';')
            target_table = config_row['target_table_name']
            target_fk_col = config_row['foreign_key_column']
            
            full_source_name = f'"{schema_name}"."{source_table}"'
            full_target_name = f'"{schema_name}"."{target_table}"'
            
            logger.debug(f"--- Starting task: {source_table} -> {target_table} ---")

            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS {full_target_name};'))
                create_sql = f"""
                    CREATE TABLE {full_target_name} (
                        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        {target_fk_col} INT NOT NULL,
                        media_type NVARCHAR(255) NOT NULL,
                        file_path NVARCHAR(MAX) NOT NULL,
                        mime_type NVARCHAR(100) NULL,
                        CONSTRAINT fk_{target_table}_{source_table} 
                            FOREIGN KEY ({target_fk_col}) REFERENCES {full_source_name}({source_pk})
                    );
                """
                conn.execute(text(create_sql))

            select_cols = f'"{source_pk}", ' + ', '.join([f'"{col}"' for col in media_cols])
            source_df = pd.read_sql(f'SELECT {select_cols} FROM {full_source_name}', engine)
            
            melted_df = source_df.melt(id_vars=[source_pk], value_vars=media_cols, var_name='media_type', value_name='cell_value')
            melted_df.dropna(subset=['cell_value'], inplace=True)
            melted_df = melted_df[melted_df['cell_value'].astype(str).str.strip().ne('[]')]
            
            if melted_df.empty:
                logger.debug(f"No media data for {source_table}. Table {target_table} will be empty.")
                continue

            # Use the new flexible parser to handle all supported JSON formats
            melted_df['file_path'] = melted_df['cell_value'].apply(parse_flexible_media_json)
            output_df = melted_df.explode('file_path').dropna(subset=['file_path'])
            
            if output_df.empty:
                logger.warning(f"No valid file paths found after parsing for {source_table}.")
                continue

            output_df.rename(columns={source_pk: target_fk_col}, inplace=True)
            output_df['file_path'] = output_df['file_path'].astype(str).str.strip().str.strip('"')
            output_df['mime_type'] = output_df['file_path'].apply(get_mime_type)

            final_cols = [target_fk_col, 'media_type', 'file_path', 'mime_type']
            output_df = output_df[final_cols].copy()
            output_df.dropna(subset=['file_path', target_fk_col], inplace=True)
            output_df = output_df[output_df['file_path'] != '']
            
            if not output_df.empty:
                logger.debug(f"Inserting {len(output_df)} records into {full_target_name}...")
                output_df.to_sql(name=target_table, con=engine, schema=schema_name, if_exists='append', index=False, chunksize=500)
            
            logger.debug(f"--- Successfully completed task: {source_table} -> {target_table} ---")

        except Exception as e:
            logger.error(f"Error on row {index} ({source_table}): {e}", exc_debug=True)
            continue
            
    logger.debug("All media lookup tasks from configuration file processed.")


# --- ORIGINAL STATIC FUNCTION (FULLY FIXED) ---

def create_lookup_project_to_media(target_db_url, schema_name):
    """
    Creates the original lookup_project_to_media table.
    This version is fixed to correctly parse all supported JSON formats.
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
        with engine.begin() as conn:
            logger.debug(f"Setting up fresh target table {full_target_name}...")
            conn.execute(text(f'DROP TABLE IF EXISTS {full_target_name};'))
            create_sql = f"""
                CREATE TABLE {full_target_name} (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    project_summary_id INT NOT NULL,
                    lookup_project_to_phase_id INT NULL,
                    media_type NVARCHAR(255) NOT NULL,
                    file_path NVARCHAR(MAX) NOT NULL,
                    mime_type NVARCHAR(100) NULL,
                    CONSTRAINT fk_media_to_summary_final FOREIGN KEY (project_summary_id) REFERENCES {full_source_name}(id),
                    CONSTRAINT fk_media_to_phase_final FOREIGN KEY (lookup_project_to_phase_id) REFERENCES {full_lookup_name}(id)
                );
            """
            conn.execute(text(create_sql))

        source_df = pd.read_sql(f'SELECT id, project_name, period, {", ".join(MEDIA_COLUMNS)} FROM {full_source_name}', engine)
        lookup_df = pd.read_sql(f'SELECT id, project_name, period, phase FROM {full_lookup_name}', engine)
        lookup_df.rename(columns={'id': 'lookup_project_to_phase_id'}, inplace=True)

        melted_df = source_df.melt(id_vars=['id', 'project_name', 'period'], value_vars=MEDIA_COLUMNS, var_name='media_type', value_name='cell_value')
        melted_df.rename(columns={'id': 'project_summary_id'}, inplace=True)

        melted_df.dropna(subset=['cell_value'], inplace=True)
        melted_df['cell_value_str'] = melted_df['cell_value'].astype(str).str.strip()
        melted_df = melted_df[melted_df['cell_value_str'].ne('[]') & melted_df['cell_value_str'].ne('')]

        if melted_df.empty:
            logger.warning("No valid media file data found to process.")
            return

        is_json_with_phase = melted_df['cell_value_str'].str.startswith('[{"')
        df_json_phase = melted_df[is_json_with_phase].copy()
        df_simple_or_obj = melted_df[~is_json_with_phase].copy()
        
        final_dfs = []

        if not df_json_phase.empty:
            logger.debug(f"Processing {len(df_json_phase)} JSON with phase format rows...")
            df_json_phase['parsed'] = df_json_phase['cell_value'].apply(parse_json_cell_with_phase)
            df_json_phase = df_json_phase.explode('parsed').dropna(subset=['parsed'])
            df_json_phase.reset_index(drop=True, inplace=True)

            parsed_df = pd.json_normalize(df_json_phase['parsed'])
            df_json_phase = pd.concat([df_json_phase.drop(columns=['parsed', 'cell_value', 'cell_value_str']), parsed_df], axis=1)

            for key in ['project_name', 'period', 'phase']:
                df_json_phase[key] = df_json_phase[key].astype(str).str.strip()
                lookup_df[key] = lookup_df[key].astype(str).str.strip()
            
            merged_json = pd.merge(df_json_phase, lookup_df, on=['project_name', 'period', 'phase'], how='left')
            final_dfs.append(merged_json)

        if not df_simple_or_obj.empty:
            logger.debug(f"Processing {len(df_simple_or_obj)} simple/object format rows...")
            # Use the robust flexible parser instead of fragile string manipulation
            df_simple_or_obj['file_path'] = df_simple_or_obj['cell_value'].apply(parse_flexible_media_json)
            df_simple_or_obj = df_simple_or_obj.explode('file_path')
            df_simple_or_obj['lookup_project_to_phase_id'] = None
            final_dfs.append(df_simple_or_obj)

        final_dfs = [df for df in final_dfs if not df.empty]
        if not final_dfs:
            logger.warning("No records were generated after processing.")
            return

        output_df = pd.concat(final_dfs, ignore_index=True)
        output_df['file_path'] = output_df['file_path'].astype(str).str.strip().str.strip('"')
        output_df['mime_type'] = output_df['file_path'].apply(get_mime_type)
        
        final_cols = ['project_summary_id', 'lookup_project_to_phase_id', 'media_type', 'file_path', 'mime_type']
        output_df = output_df[final_cols]
        output_df.dropna(subset=['file_path'], inplace=True)
        output_df = output_df[output_df['file_path'] != '']

        logger.debug(f"Processed data. Inserting {len(output_df)} records into the database...")
        output_df.to_sql(name='lookup_project_to_media', con=engine, schema=schema_name, if_exists='append', index=False, chunksize=400)
        
        logger.debug("Successfully created and populated the media lookup table.")

    except Exception as e:
        logger.error(f"A critical error occurred during media lookup creation: {e}", exc_debug=True)
        raise