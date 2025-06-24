import json

import pandas as pd
from sqlalchemy import create_engine, text

from utils.logger import setup_logger

logger = setup_logger()

def create_project_status_mapping(target_db_url, schema_name):
    """
    Create mapping table for project status predecessor/successor relationships.
    Processes data family by family to handle large tables efficiently.
    
    Steps:
    1. Check table structure and get available columns
    2. Get unique project names
    3. For each project, get all periods
    4. Process each family (project_name + period) separately
    5. Create slugs and resolve relationships
    6. Insert mapping records in batches
    """
    
    engine = create_engine(target_db_url)
    
    try:
        with engine.begin() as conn:
            # Step 1: Check table structure and get available columns
            logger.debug("Checking table structure...")
            available_columns = get_table_columns(conn, schema_name)
            
            # Step 2: Create the mapping table first
            logger.debug("Creating mapping table...")
            create_mapping_table(conn, schema_name)
            
            # Step 3: Get unique project names
            logger.debug("Getting unique project names...")
            
            projects_query = text(f"""
                SELECT DISTINCT project_name
                FROM {schema_name}.project_status
                WHERE project_name IS NOT NULL
                ORDER BY project_name
            """)
            
            projects_df = pd.read_sql(projects_query, conn)
            project_names = projects_df['project_name'].tolist()
            
            logger.debug(f"Found {len(project_names)} unique projects to process")
            
            total_mappings = 0
            
            # Step 4: Process each project
            for i, project_name in enumerate(project_names, 1):
                logger.debug(f"Processing project {i}/{len(project_names)}: '{project_name}'")
                
                try:
                    # Get all periods for this project
                    periods_query = text(f"""
                        SELECT DISTINCT period
                        FROM {schema_name}.project_status
                        WHERE project_name = :project_name
                        AND period IS NOT NULL
                        ORDER BY period
                    """)
                    
                    periods_df = pd.read_sql(periods_query, conn, params={'project_name': project_name})
                    periods = periods_df['period'].tolist()
                    
                    logger.debug(f"  Found {len(periods)} periods for project '{project_name}'")
                    
                    # Step 5: Process each family (project + period)
                    for period in periods:
                        logger.debug(f"  Processing family: '{project_name}' - {period}")
                        
                        family_mappings = process_family(conn, schema_name, project_name, period, available_columns)
                        
                        if family_mappings:
                            # Insert mappings for this family
                            insert_mappings_batch(conn, schema_name, family_mappings)
                            total_mappings += len(family_mappings)
                            logger.debug(f"    Inserted {len(family_mappings)} mappings for this family")
                        
                except Exception as e:
                    logger.debug(f"Error processing project '{project_name}': {e}")
                    # Continue with next project instead of failing completely
                    continue
            
            logger.debug(f"Successfully processed all projects. Total mappings created: {total_mappings}")
                
    except Exception as e:
        logger.error(f"Error creating project status mapping: {e}")
        raise


def get_table_columns(conn, schema_name):
    """
    Get the available columns in the project_status table.
    """
    columns_query = text(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema_name 
        AND TABLE_NAME = 'project_status'
        ORDER BY ORDINAL_POSITION
    """)
    
    columns_df = pd.read_sql(columns_query, conn, params={'schema_name': schema_name})
    return columns_df['COLUMN_NAME'].tolist()


def process_family(conn, schema_name, project_name, period, available_columns):
    """
    Process a single family (project_name + period) and return mapping records.
    """
    try:
        # Build the SELECT clause based on available columns
        slug_columns = ['project_phase_category', 'phase', 'stage_status', 'sub_stage']
        available_slug_columns = [col for col in slug_columns if col in available_columns]
        
        if not available_slug_columns:
            logger.warning(f"No slug columns found for family {project_name}, {period}")
            return []
        
        # Build the query dynamically
        select_columns = ['id'] + available_slug_columns + ['predecessor', 'successor']
        select_clause = ', '.join(select_columns)
        
        family_query = text(f"""
            SELECT {select_clause}
            FROM {schema_name}.project_status
            WHERE project_name = :project_name 
            AND period = :period
            ORDER BY id
        """)
        
        family_df = pd.read_sql(family_query, conn, params={
            'project_name': project_name, 
            'period': period
        })
        
        if family_df.empty:
            return []
        
        logger.debug(f"    Processing {len(family_df)} records in family")
        
        # Create slug lookup for this family
        slug_to_id = {}
        
        for idx, row in family_df.iterrows():
            # Build slug from available non-null columns
            slug_parts = []
            
            for col in available_slug_columns:
                value = row[col]
                if pd.notna(value) and str(value).strip():
                    slug_parts.append(str(value).strip())
            
            slug = '$#'.join(slug_parts)
            
            if slug in slug_to_id:
                raise ValueError(f"Duplicate slug '{slug}' found in family {project_name}, {period}")
            
            slug_to_id[slug] = row['id']
        
        # Process relationships for each row
        mapping_records = []
        
        for idx, row in family_df.iterrows():
            current_id = row['id']
            
            # Process predecessors
            if 'predecessor' in family_df.columns and pd.notna(row['predecessor']) and str(row['predecessor']).strip():
                predecessor_slugs = parse_relationship_column(row['predecessor'])
                
                # Skip if empty array or no valid slugs
                if predecessor_slugs:
                    for slug in predecessor_slugs:
                        if slug not in slug_to_id:
                            raise ValueError(f"Predecessor slug '{slug}' not found in family {project_name}, {period}")
                        
                        mapping_records.append({
                            'project_status_id': current_id,
                            'destination_project_status_id': slug_to_id[slug],
                            'association_type': 'predecessor'
                        })
            
            # Process successors
            if 'successor' in family_df.columns and pd.notna(row['successor']) and str(row['successor']).strip():
                successor_slugs = parse_relationship_column(row['successor'])
                
                # Skip if empty array or no valid slugs
                if successor_slugs:
                    for slug in successor_slugs:
                        if slug not in slug_to_id:
                            raise ValueError(f"Successor slug '{slug}' not found in family {project_name}, {period}")
                        
                        mapping_records.append({
                            'project_status_id': current_id,
                            'destination_project_status_id': slug_to_id[slug],
                            'association_type': 'successor'
                        })
        
        return mapping_records
        
    except Exception as e:
        logger.error(f"Error processing family {project_name}, {period}: {e}")
        raise


def create_mapping_table(conn, schema_name):
    """
    Create the mapping table with proper constraints.
    """
    # Drop table if exists and recreate
    conn.execute(text(f"""
        IF OBJECT_ID('{schema_name}.mapping_project_status_predecessor_successor', 'U') IS NOT NULL
            DROP TABLE {schema_name}.mapping_project_status_predecessor_successor
    """))
    
    # Create the mapping table
    conn.execute(text(f"""
        CREATE TABLE {schema_name}.mapping_project_status_predecessor_successor (
            project_status_id INT NOT NULL,
            destination_project_status_id INT NOT NULL,
            association_type VARCHAR(20) NOT NULL CHECK (association_type IN ('predecessor', 'successor')),
            CONSTRAINT FK_mapping_project_status_source 
                FOREIGN KEY (project_status_id) REFERENCES {schema_name}.project_status(id),
            CONSTRAINT FK_mapping_project_status_destination 
                FOREIGN KEY (destination_project_status_id) REFERENCES {schema_name}.project_status(id),
            CONSTRAINT PK_mapping_project_status 
                PRIMARY KEY (project_status_id, destination_project_status_id, association_type)
        )
    """))
    
    logger.debug("Mapping table created successfully")


def insert_mappings_batch(conn, schema_name, mapping_records):
    """
    Insert mapping records in batches for better performance.
    """
    if not mapping_records:
        return
    
    batch_size = 1000
    
    for i in range(0, len(mapping_records), batch_size):
        batch = mapping_records[i:i+batch_size]
        
        # Prepare batch insert
        values_list = []
        for record in batch:
            values_list.append(f"({record['project_status_id']}, {record['destination_project_status_id']}, '{record['association_type']}')")
        
        values_str = ',\n'.join(values_list)
        
        insert_query = text(f"""
            INSERT INTO {schema_name}.mapping_project_status_predecessor_successor 
            (project_status_id, destination_project_status_id, association_type)
            VALUES {values_str}
        """)
        
        conn.execute(insert_query)


def parse_relationship_column(column_value):
    """
    Parse predecessor/successor column value to extract slugs.
    
    Input format: '["phase 3$#Initiation", "another$#slug"]' or '[]' (empty)
    Output: List of slugs (empty list if no valid slugs found)
    """
    if pd.isna(column_value) or not str(column_value).strip():
        return []
    
    try:
        # Clean the string - remove extra whitespace
        clean_value = str(column_value).strip()
        
        # Handle empty array case explicitly
        if clean_value == '[]':
            return []
        
        # Handle different possible formats
        if clean_value.startswith('[') and clean_value.endswith(']'):
            # JSON array format
            try:
                slugs_list = json.loads(clean_value)
                # Filter out empty strings and None values
                valid_slugs = [slug.strip() for slug in slugs_list if slug and str(slug).strip()]
                return valid_slugs
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON array: {clean_value}")
                return []
        else:
            # Comma-separated format or single value
            if ',' in clean_value:
                valid_slugs = [slug.strip() for slug in clean_value.split(',') if slug.strip()]
                return valid_slugs
            else:
                return [clean_value.strip()] if clean_value.strip() else []
                
    except Exception as e:
        logger.error(f"Error parsing relationship column: {e}")
        return []


def verify_mapping_results(target_db_url, schema_name):
    """
    Verify the mapping table results and provide summary statistics.
    """
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        # Get summary statistics
        stats_query = text(f"""
            SELECT 
                association_type,
                COUNT(*) as relationship_count,
                COUNT(DISTINCT project_status_id) as unique_source_projects,
                COUNT(DISTINCT destination_project_status_id) as unique_destination_projects
            FROM {schema_name}.mapping_project_status_predecessor_successor
            GROUP BY association_type
            
            UNION ALL
            
            SELECT 
                'TOTAL' as association_type,
                COUNT(*) as relationship_count,
                COUNT(DISTINCT project_status_id) as unique_source_projects,
                COUNT(DISTINCT destination_project_status_id) as unique_destination_projects
            FROM {schema_name}.mapping_project_status_predecessor_successor
        """)
        
        stats_df = pd.read_sql(stats_query, conn)
        
        logger.debug("Mapping table statistics:")
        for _, row in stats_df.iterrows():
            logger.debug(f"{row['association_type']}: {row['relationship_count']} relationships, "
                       f"{row['unique_source_projects']} source projects, "
                       f"{row['unique_destination_projects']} destination projects")
        
        return stats_df


def get_processing_progress(target_db_url, schema_name):
    """
    Get progress information about how many projects have been processed.
    """
    engine = create_engine(target_db_url)
    
    with engine.begin() as conn:
        # Get total projects
        total_projects_query = text(f"""
            SELECT COUNT(DISTINCT project_name) as total_projects
            FROM {schema_name}.project_status
            WHERE project_name IS NOT NULL
        """)
        
        total_projects = pd.read_sql(total_projects_query, conn).iloc[0]['total_projects']
        
        # Get processed projects (those with mappings)
        processed_projects_query = text(f"""
            SELECT COUNT(DISTINCT ps.project_name) as processed_projects
            FROM {schema_name}.project_status ps
            INNER JOIN {schema_name}.mapping_project_status_predecessor_successor m
                ON ps.id = m.project_status_id
        """)
        
        processed_projects = pd.read_sql(processed_projects_query, conn).iloc[0]['processed_projects']
        
        logger.debug(f"Processing progress: {processed_projects}/{total_projects} projects completed")
        
        return {
            'total_projects': total_projects,
            'processed_projects': processed_projects,
            'completion_percentage': (processed_projects / total_projects * 100) if total_projects > 0 else 0
        }


def implement_predecessor_successor(target_db_url, schema_name):
    # Create the mapping
    create_project_status_mapping(target_db_url, schema_name)
    
    # Verify results
    verify_mapping_results(target_db_url, schema_name)
    
    # Show progress
    get_processing_progress(target_db_url, schema_name)

