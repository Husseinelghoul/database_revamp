import json
from sqlalchemy.engine import Connection

import pandas as pd
from sqlalchemy import create_engine, text

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

    - Links to project_status on 'project_name', 'period', 'phase', 'stage_status', and 'sub_stage'.
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
                    /* Correctly handle potential NULLs in the following columns */
                    AND (pm.phase = ps.phase OR (pm.phase IS NULL AND ps.phase IS NULL))
                    AND (pm.stage_status = ps.stage_status OR (pm.stage_status IS NULL AND ps.stage_status IS NULL))
                    AND (pm.sub_stage = ps.sub_stage OR (pm.sub_stage IS NULL AND ps.sub_stage IS NULL));
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

def create_lookup_project_to_project_phase_category(target_db_url: str,schema_name: str):
    """
    Processes project data to create a lookup table for project phase categories.

    This function connects to a database, drops the existing lookup table, and then
    reads the 'project_status' table. It processes the data project by project
    to manage memory usage. For each project, it extracts the 'project_phase_category'
    values, which are delimited by '$@'. It splits these values, removes duplicates,
    and stores the unique combinations of period, project name, and category in the
    new 'lookup_project_to_project_phase_category' table.

    Args:
        schema_name (str): The name of the database schema where the source
                           and target tables reside.
        target_db_url (str): The database connection URL in a SQLAlchemy-compatible
                             format (e.g., 'mssql+pyodbc://user:pass@dsn').
    """
    # --- Input Validation ---
    # This check helps prevent errors if the function arguments are swapped.
    if "://" not in target_db_url:
        error_msg = (
            f"Invalid `target_db_url` provided: '{target_db_url}'. "
            "This does not look like a valid SQLAlchemy connection string. "
            "It should be in a format like 'dialect+driver://user:pass@host/database'. "
            "Please check if the arguments for schema_name and target_db_url were swapped during the function call."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    new_table_name = 'lookup_project_to_project_phase_category'
    source_table_name = 'project_status'
    table_created_flag = False # Flag to check if the table has been created

    try:
        # --- Database Connection ---
        # Using fast_executemany can significantly speed up inserts for some drivers like pyodbc.
        engine = create_engine(target_db_url, fast_executemany=True)
        logger.debug("Successfully connected to the database.")

        with engine.connect() as connection:
            # --- Drop the target table if it already exists for a clean run ---
            logger.debug(f"Attempting to drop table '{schema_name}.{new_table_name}' if it exists.")
            # Using a transaction to ensure the drop command is committed.
            with connection.begin():
                 # Note: "IF EXISTS" syntax is common but might vary slightly between SQL dialects.
                connection.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{new_table_name}"))
            logger.debug("Table dropped or did not exist.")


            # --- Step 1: Get all unique project names to process iteratively ---
            logger.debug(f"Fetching unique project names from '{schema_name}.{source_table_name}'.")
            project_query = text(f"SELECT DISTINCT project_name FROM {schema_name}.{source_table_name} WHERE project_name IS NOT NULL")
            unique_projects_df = pd.read_sql(project_query, connection)
            project_names = unique_projects_df['project_name'].tolist()
            logger.debug(f"Found {len(project_names)} unique projects to process.")

            # --- Step 2: Process each project individually ---
            for project_name in project_names:
                logger.debug(f"Processing project: '{project_name}'")

                # --- Fetch data for the current project, using the correct source column ---
                query = text(f"""
                    SELECT period, project_name, project_phase_category
                    FROM {schema_name}.{source_table_name}
                    WHERE project_name = :project_name AND project_phase_category IS NOT NULL
                """)
                project_df = pd.read_sql(query, connection, params={'project_name': project_name})

                if project_df.empty:
                    logger.debug(f"No data found for project '{project_name}'. Skipping.")
                    continue

                # --- Data Transformation ---
                # Split the category string by the delimiter '$@', escaping the special '$' character.
                project_df['project_phase_category'] = project_df['project_phase_category'].str.split(r'\$@')

                # Explode the DataFrame to have one category per row
                exploded_df = project_df.explode('project_phase_category')

                # Remove leading/trailing whitespace from the new category column
                exploded_df['project_phase_category'] = exploded_df['project_phase_category'].str.strip()

                # Drop duplicate rows to get unique category assignments for the project
                unique_categories_df = exploded_df.drop_duplicates().reset_index(drop=True)

                # --- Load data into the new table ---
                if not unique_categories_df.empty:
                    # Since we drop the table at the start, we can always append.
                    # The first write operation will create the new table.
                    unique_categories_df.to_sql(
                        name=new_table_name,
                        con=engine,
                        schema=schema_name,
                        if_exists='append',
                        index=False # We will add our own identity column
                    )

                    if not table_created_flag:
                        # After creating the table with the first batch of data,
                        # we add the 'id' identity column.
                        try:
                            with engine.begin() as conn:
                                # This syntax is for SQL Server. It might need adjustment for other DBs.
                                conn.execute(text(f"ALTER TABLE {schema_name}.{new_table_name} ADD id INT IDENTITY(1,1) PRIMARY KEY;"))
                            logger.debug(f"Created and configured identity column on '{schema_name}.{new_table_name}'.")
                        except Exception as e:
                            logger.error(f"Could not configure identity column. You may need to do this manually. Error: {e}")
                        table_created_flag = True # Ensure this runs only once

                    logger.debug(f"Successfully inserted {len(unique_categories_df)} rows for project '{project_name}'.")

    except Exception as e:
        logger.error(f"An error occurred during the process: {e}")
        # Optionally re-raise the exception if you want the script to stop
        raise e

def merge_project_status_columns(target_db_url, schema_name):
    """
    Merges and standardizes project status data into four new columns within
    the project_status table.

    This function performs two main operations:
    1.  Ensures the target table `"{schema_name}"."project_status"` contains four
        new columns:
        - baseline_plan_finish_date (DATE)
        - forecast_finish_date (DATE)
        - planned_progress_percentage (FLOAT)
        - actual_progress_percentage (FLOAT)

    2.  Populates these new columns based on the value of the `stage_status`
        column, consolidating data from different fields into a
        standardized format.
    """
    engine = create_engine(target_db_url)
    full_ps_table = f'"{schema_name}"."project_status"'

    try:
        # Step 1: Ensure the new columns exist in the project_status table.
        # This is done idempotently, so it's safe to run multiple times.
        with engine.begin() as conn:
            logger.debug(f"Ensuring required columns exist in {full_ps_table}...")

            # Add baseline_plan_finish_date if it doesn't exist
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_status', 'baseline_plan_finish_date') IS NULL
                BEGIN
                    ALTER TABLE {full_ps_table} ADD baseline_plan_finish_date DATE NULL;
                    PRINT 'Column baseline_plan_finish_date added.';
                END
            """))

            # Add forecast_finish_date if it doesn't exist
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_status', 'forecast_finish_date') IS NULL
                BEGIN
                    ALTER TABLE {full_ps_table} ADD forecast_finish_date DATE NULL;
                    PRINT 'Column forecast_finish_date added.';
                END
            """))

            # Add planned_progress_percentage if it doesn't exist
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_status', 'planned_progress_percentage') IS NULL
                BEGIN
                    ALTER TABLE {full_ps_table} ADD planned_progress_percentage FLOAT NULL;
                    PRINT 'Column planned_progress_percentage added.';
                END
            """))

            # Add actual_progress_percentage if it doesn't exist
            conn.execute(text(f"""
                IF COL_LENGTH('{schema_name}.project_status', 'actual_progress_percentage') IS NULL
                BEGIN
                    ALTER TABLE {full_ps_table} ADD actual_progress_percentage FLOAT NULL;
                    PRINT 'Column actual_progress_percentage added.';
                END
            """))
            
            logger.debug("Column existence check complete.")

        # Step 2: Update the new columns using a single, efficient SQL statement
        # with CASE expressions to handle the conditional logic.
        with engine.begin() as conn:
            logger.debug(f"Updating merged columns in {full_ps_table}...")

            update_sql = text(f"""
                UPDATE {full_ps_table}
                SET
                    baseline_plan_finish_date = CASE
                        WHEN stage_status IN ('Initiation', 'Design', 'LDC Procurement') THEN baseline_plan_finish
                        WHEN stage_status IN ('Contractor Procurement', 'Construction', 'DLP and Project Closeout') THEN plan_end_date
                        ELSE NULL
                    END,
                    forecast_finish_date = CASE
                        WHEN stage_status IN ('Initiation', 'Design', 'LDC Procurement') THEN forecast_finish
                        WHEN stage_status IN ('Contractor Procurement', 'Construction', 'DLP and Project Closeout') THEN forecasted_end_date
                        ELSE NULL
                    END,
                    planned_progress_percentage = CASE
                        WHEN stage_status IN ('Initiation', 'Design', 'LDC Procurement') THEN rev_plan_percentage
                        WHEN stage_status IN ('Contractor Procurement', 'Construction', 'DLP and Project Closeout') THEN revised_plan_percentage
                        ELSE NULL
                    END,
                    actual_progress_percentage = CASE
                        WHEN stage_status IN ('Initiation', 'Design', 'LDC Procurement') THEN actual_percentage
                        WHEN stage_status IN ('Contractor Procurement', 'Construction', 'DLP and Project Closeout') THEN actual_plan_percentage
                        ELSE NULL
                    END;
            """)
            
            result = conn.execute(update_sql)
            logger.debug(f"Updated {result.rowcount} rows in {full_ps_table}.")

        logger.debug("Project status column merge process completed successfully.")

    except Exception as e:
        logger.error(f"A critical error occurred during the column merge process: {e}", exc_info=True)
        # Re-raise the exception to allow the calling code to handle it
        raise
