import json
import os

import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger

# Assume logger is configured
logger = setup_logger()

def create_mapping_table_no_constraints(conn, schema_name):
    """
    Creates the mapping table WITHOUT keys or constraints for fast inserts.
    """
    table_name = "mapping_project_status_predecessor_successor"
    full_table_name = f'"{schema_name}"."{table_name}"'
    
    logger.debug(f"Creating fresh mapping table {full_table_name} without constraints...")
    conn.execute(text(f'DROP TABLE IF EXISTS {full_table_name};'))
    conn.execute(text(f"""
        CREATE TABLE {full_table_name} (
            project_status_id INT,
            destination_project_status_id INT,
            association_type VARCHAR(20)
        );
    """))
    logger.debug("Plain mapping table created successfully.")

def add_constraints_to_mapping_table(conn, schema_name):
    """
    Adds all keys and constraints to the mapping table after data has been loaded.
    This is much more performant than inserting into a constrained table.
    """
    table_name = "mapping_project_status_predecessor_successor"
    source_table_name = "project_status"
    full_table_name = f'"{schema_name}"."{table_name}"'

    logger.debug("Finalizing mapping table: Removing duplicates and adding constraints...")
    
    # Step 1: Remove any potential duplicates that were inserted across batches
    deduplicate_sql = f"""
        ;WITH CTE AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY project_status_id, destination_project_status_id, association_type ORDER BY (SELECT NULL)) as rn
            FROM {full_table_name}
        )
        DELETE FROM CTE WHERE rn > 1;
    """
    result = conn.execute(text(deduplicate_sql))
    logger.debug(f"Removed {result.rowcount} duplicate mapping rows.")

    # Step 2: Add all constraints at once
    try:
        conn.execute(text(f'ALTER TABLE {full_table_name} ALTER COLUMN project_status_id INT NOT NULL;'))
        conn.execute(text(f'ALTER TABLE {full_table_name} ALTER COLUMN destination_project_status_id INT NOT NULL;'))
        conn.execute(text(f'ALTER TABLE {full_table_name} ALTER COLUMN association_type VARCHAR(20) NOT NULL;'))
        
        pk_name = f"PK_{table_name}"
        conn.execute(text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{pk_name}" PRIMARY KEY (project_status_id, destination_project_status_id, association_type);'))

        fk_source_name = f"FK_{table_name}_source"
        conn.execute(text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{fk_source_name}" FOREIGN KEY (project_status_id) REFERENCES "{schema_name}"."{source_table_name}"(id);'))
        
        fk_dest_name = f"FK_{table_name}_destination"
        conn.execute(text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{fk_dest_name}" FOREIGN KEY (destination_project_status_id) REFERENCES "{schema_name}"."{source_table_name}"(id);'))
        
        logger.debug("Successfully added all keys and constraints to mapping table.")
    except Exception as e:
        logger.error(f"Failed to add constraints. The data may contain invalid relationships (e.g., pointing to a non-existent ID). Error: {e}")
        raise

def implement_predecessor_successor(target_db_url, schema_name):
    """
    The definitive high-performance version. It batches by project and applies
    constraints at the very end to maximize insert speed.
    """
    # *** OPTIMIZATION: Enable fast_executemany for pyodbc driver ***
    engine = create_engine(target_db_url, fast_executemany=True)
    
    logger.debug("Starting optimized predecessor/successor mapping...")

    try:
        # The main transaction block is for reading and inserting.
        with engine.begin() as conn:
            # === Step 1: Create a plain, unconstrained table for fast inserts ===
            create_mapping_table_no_constraints(conn, schema_name)

            # === Step 2: Get a list of all unique projects to iterate through ===
            logger.debug("Fetching list of all unique projects...")
            projects_query = text(f'SELECT DISTINCT project_name FROM "{schema_name}"."project_status" WHERE project_name IS NOT NULL')
            project_names = [row[0] for row in conn.execute(projects_query)]
            logger.debug(f"Found {len(project_names)} projects to process.")
            
            # === Step 3: Loop through each project, processing and inserting into the plain table ===
            for i, project_name in enumerate(project_names, 1):
                logger.debug(f"Processing project {i}/{len(project_names)}: {project_name}")
                
                project_df = get_project_data(conn, schema_name, project_name)
                if project_df.empty:
                    continue
                
                mappings_df = process_project_dataframe(project_df)
                if mappings_df.empty:
                    continue

                logger.debug(f"  Found {len(mappings_df)} mappings for '{project_name}'. Inserting...")
                
                # *** FIX: Pass the active connection `conn` and use a safe chunksize. ***
                # This ensures all inserts happen within the single parent transaction and respects driver limits.
                mappings_df.to_sql(
                    name='mapping_project_status_predecessor_successor',
                    con=conn, # Use the existing connection/transaction
                    schema=schema_name,
                    if_exists='append',
                    index=False,
                    chunksize=500, # Safe chunk size to stay under the 2100 parameter limit
                    method='multi' 
                )
        
        # === Step 4: After all data is loaded, run the finalization step in a new transaction ===
        with engine.begin() as conn:
            add_constraints_to_mapping_table(conn, schema_name)

        logger.debug("Predecessor/successor mapping completed successfully.")

    except Exception as e:
        logger.error(f"A critical error occurred during the mapping process: {e}", exc_info=True)
        raise

def get_project_data(conn, schema_name, project_name):
    """Helper function to fetch data for one project."""
    slug_columns = ['project_phase_category', 'phase', 'stage_status', 'sub_stage']
    query_columns = ['id', 'predecessor', 'successor'] + slug_columns
    
    project_sql = text(f"""
        SELECT {', '.join(f'"{col}"' for col in query_columns)}
        FROM "{schema_name}"."project_status"
        WHERE project_name = :project_name
    """)
    return pd.read_sql(project_sql, conn, params={'project_name': project_name})

def process_project_dataframe(df):
    """Helper function to normalize a project's dataframe."""
    if df.empty:
        return pd.DataFrame()

    slug_columns = ['project_phase_category', 'phase', 'stage_status', 'sub_stage']
    
    def create_slug(row):
        return '$#'.join(row.dropna().astype(str).str.strip())
    df['slug'] = df[slug_columns].apply(create_slug, axis=1)
    slug_to_id = pd.Series(df.id.values, index=df.slug).to_dict()

    # Process predecessors
    predecessor_df = df[df['predecessor'].notna() & (df['predecessor'].str.strip() != '[]')].copy()
    if not predecessor_df.empty:
        # Use a vectorized approach for parsing JSON for better performance
        predecessor_df['predecessor'] = [json.loads(s) for s in predecessor_df['predecessor']]
        predecessor_df = predecessor_df.explode('predecessor')
        predecessor_df['destination_project_status_id'] = predecessor_df['predecessor'].map(slug_to_id)
        predecessor_df['association_type'] = 'predecessor'
    
    # Process successors
    successor_df = df[df['successor'].notna() & (df['successor'].str.strip() != '[]')].copy()
    if not successor_df.empty:
        successor_df['successor'] = [json.loads(s) for s in successor_df['successor']]
        successor_df = successor_df.explode('successor')
        successor_df['destination_project_status_id'] = successor_df['successor'].map(slug_to_id)
        successor_df['association_type'] = 'successor'

    final_mappings = pd.concat([
        predecessor_df[['id', 'destination_project_status_id', 'association_type']] if not predecessor_df.empty else pd.DataFrame(),
        successor_df[['id', 'destination_project_status_id', 'association_type']] if not successor_df.empty else pd.DataFrame()
    ])

    if final_mappings.empty:
        return pd.DataFrame()

    final_mappings.rename(columns={'id': 'project_status_id'}, inplace=True)
    final_mappings.dropna(inplace=True)
    final_mappings['destination_project_status_id'] = final_mappings['destination_project_status_id'].astype(int)
    
    return final_mappings

def link_project_management_to_status(target_db_url, schema_name):
    """
    Simplified and Final Version: Updates project_management by directly joining
    to project_status on project_name, period, and phase. No slugs involved.
    """
    engine = create_engine(target_db_url)
    full_pm_table = f'"{schema_name}"."project_management"'
    full_ps_table = f'"{schema_name}"."project_status"'

    try:
        # Step 1: Ensure the target column exists.
        with engine.begin() as conn:
            logger.debug(f"Ensuring {full_pm_table} has the project_status_id column...")
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_management', 'project_status_id') IS NULL
                BEGIN
                    ALTER TABLE {full_pm_table} ADD project_status_id INT NULL;
                END
            """))

        # Step 2: Perform the entire update with a single, efficient SQL statement.
        with engine.begin() as conn:
            logger.debug("Updating project_management by joining directly to project_status...")
            
            # This single query does all the work on the database side.
            update_sql = text(f"""
                UPDATE pm
                SET
                    pm.project_status_id = ps.id
                FROM
                    {full_pm_table} AS pm
                JOIN
                    {full_ps_table} AS ps ON pm.project_name = ps.project_name
                                         AND pm.period = ps.period
                                         AND pm.phase = ps.phase;
            """)
            
            result = conn.execute(update_sql)
            logger.info(f"Successfully updated {result.rowcount} rows in project_management.")

        logger.debug("Project management linking process completed successfully.")

    except Exception as e:
        logger.error(f"A critical error occurred during the project management linking process: {e}", exc_info=True)
        raise

import pandas as pd
from sqlalchemy import create_engine, text
import logging
import json
import os

# Assume logger is configured and available
logger = logging.getLogger(__name__)

def get_mime_type(path):
    """Helper to safely extract a file extension."""
    if not isinstance(path, str) or '.' not in path:
        return None
    return os.path.splitext(path)[1].lstrip('.').lower()

def parse_json_cell(cell_value):
    """Safely parse a JSON cell and extract file_path and phase."""
    try:
        data = json.loads(cell_value)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [{'file_path': str(item.get('file_path', '')).strip('"'), 'phase': str(item.get('phase', '')).strip()} for item in data]
    except (json.JSONDecodeError, TypeError):
        pass
    return []

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