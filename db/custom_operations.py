import json
import pandas as pd
from sqlalchemy import create_engine, text, exc

from utils.logger import setup_logger

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

def link_project_management_to_sources(target_db_url, schema_name):
    """
    Updates the project_management table by linking it to both the
    project_status and project_summary tables.

    - Links to project_status on 'project_name', 'period', and 'phase'.
    - Links to project_summary on 'project_name' and 'period'.
    """
    engine = create_engine(target_db_url)
    
    # Define all table names
    full_pm_table = f'"{schema_name}"."project_management"'
    full_ps_table = f'"{schema_name}"."project_status"'
    full_summary_table = f'"{schema_name}"."project_summary"'

    try:
        # Step 1: Ensure the foreign key columns exist in the project_management table.
        with engine.begin() as conn:
            logger.debug(f"Ensuring required columns exist in {full_pm_table}...")
            
            # Add project_status_id if it doesn't exist
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_management', 'project_status_id') IS NULL
                BEGIN
                    ALTER TABLE {full_pm_table} ADD project_status_id INT NULL;
                END
            """))
            
            # Add project_summary_id if it doesn't exist
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_management', 'project_summary_id') IS NULL
                BEGIN
                    ALTER TABLE {full_pm_table} ADD project_summary_id INT NULL;
                END
            """))

        # Step 2: Perform the updates using efficient SQL joins.
        with engine.begin() as conn:
            # --- Link to project_status ---
            logger.debug("Updating project_management with IDs from project_status...")
            update_status_sql = text(f"""
                UPDATE pm
                SET pm.project_status_id = ps.id
                FROM {full_pm_table} AS pm
                JOIN {full_ps_table} AS ps 
                    ON pm.project_name = ps.project_name
                    AND pm.period = ps.period
                    AND pm.phase = ps.phase;
            """)
            status_result = conn.execute(update_status_sql)
            logger.debug(f"Linked {status_result.rowcount} rows to project_status.")

            # --- Link to project_summary ---
            logger.debug("Updating project_management with IDs from project_summary...")
            update_summary_sql = text(f"""
                UPDATE pm
                SET pm.project_summary_id = psum.id
                FROM {full_pm_table} AS pm
                JOIN {full_summary_table} AS psum 
                    ON pm.project_name = psum.project_name
                    AND pm.period = psum.period;
            """)
            summary_result = conn.execute(update_summary_sql)
            logger.debug(f"Linked {summary_result.rowcount} rows to project_summary.")

        logger.debug("Project management linking process completed successfully.")

    except Exception as e:
        logger.error(f"A critical error occurred during the project management linking process: {e}", exc_info=True)
        raise

def link_previous_period_status(target_db_url, schema_name):
    """
    Finds the status from the immediate prior period and links it.

    This version uses a robust "drop and recreate" pattern to ensure a clean
    run every time, avoiding errors from previous failed attempts.
    """
    engine = create_engine(target_db_url)
    
    table_name = "project_status"
    full_table_name = f'"{schema_name}"."{table_name}"'
    target_column = "previous_period_project_status_id"
    fk_name = f"FK_{table_name}_previous_period"

    business_key_cols = [
        "project_name", "project_phase_category", "phase", 
        "stage_status", "sub_stage"
    ]

    logger.debug(f"Starting robust previous period linking for {full_table_name}...")

    try:
        with engine.begin() as conn:
            # === STEP 1: PRE-CLEANUP (Drop existing objects) ===
            logger.debug("Performing cleanup before run...")
            
            # 1a. Drop the Foreign Key first to remove dependencies on the column
            try:
                conn.execute(text(f'ALTER TABLE {full_table_name} DROP CONSTRAINT "{fk_name}";'))
                logger.debug(f"Dropped existing constraint '{fk_name}'.")
            except exc.SQLAlchemyError:
                logger.debug(f"Constraint '{fk_name}' did not exist, skipping drop.")
                pass

            # 1b. Drop the Column itself
            try:
                conn.execute(text(f'ALTER TABLE {full_table_name} DROP COLUMN {target_column};'))
                logger.debug(f"Dropped existing column '{target_column}'.")
            except exc.SQLAlchemyError:
                logger.debug(f"Column '{target_column}' did not exist, skipping drop.")
                pass

            # === STEP 2: RECREATE THE COLUMN ===
            logger.debug(f"Creating fresh column '{target_column}'.")
            conn.execute(text(f'ALTER TABLE {full_table_name} ADD {target_column} INT NULL;'))

            # === STEP 3: UPDATE THE DATA ===
            logger.debug(f"Updating '{target_column}', selecting max ID to resolve duplicates...")
            
            partition_clause = ', '.join(f'"{col}"' for col in business_key_cols)
            update_sql = text(f"""
                WITH CanonicalIDs AS (
                    SELECT {partition_clause}, period, MAX(id) as canonical_id
                    FROM {full_table_name}
                    GROUP BY {partition_clause}, period
                ),
                PreviousStatus AS (
                    SELECT
                        canonical_id,
                        LAG(canonical_id, 1) OVER (
                            PARTITION BY {partition_clause} ORDER BY period ASC
                        ) AS prev_canonical_id
                    FROM CanonicalIDs
                )
                UPDATE ps
                SET ps.{target_column} = prev.prev_canonical_id
                FROM {full_table_name} AS ps
                JOIN PreviousStatus AS prev ON ps.id = prev.canonical_id;
            """)
            result = conn.execute(update_sql)
            logger.debug(f"Successfully updated {result.rowcount} records.")
            
            # === STEP 4: RECREATE THE FOREIGN KEY ===
            logger.debug(f"Adding foreign key constraint '{fk_name}'...")
            add_fk_sql = text(f"""
                ALTER TABLE {full_table_name}
                ADD CONSTRAINT "{fk_name}"
                FOREIGN KEY ({target_column}) REFERENCES {full_table_name}(id);
            """)
            conn.execute(add_fk_sql)
            logger.debug("Successfully added foreign key constraint.")

        logger.debug("Previous period status linking process completed successfully.")

    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
        raise