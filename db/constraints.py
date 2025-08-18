from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from utils.logger import setup_logger

logger = setup_logger()


# --- Main Controller Function ---


def _drop_existing_constraints(conn, schema_name):
    """
    Finds and drops all existing CHECK, UNIQUE constraints, and UNIQUE INDEXED VIEWS.
    """
    logger.debug(f"Dropping all existing constraints and indexed views in schema '{schema_name}'...")
    
    # --- Part 1: Drop standard constraints and hash columns (your existing logic) ---
    constraints_to_drop_query = text("""...""") # Your existing query here
    # ... your existing try/except block for dropping table constraints ...

    # --- Part 2: Drop unique indexed views ---
    indexed_views_query = text("""
        SELECT s.name AS schema_name, v.name AS view_name
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE EXISTS (
            SELECT 1 FROM sys.indexes i
            WHERE i.object_id = v.object_id
              AND i.is_unique = 1 AND i.type_desc = 'CLUSTERED'
        ) AND s.name = :schema;
    """)
    try:
        views_to_drop = conn.execute(indexed_views_query, {"schema": schema_name}).fetchall()
        if views_to_drop:
            logger.debug(f"Found {len(views_to_drop)} unique indexed views to drop.")
            for s_name, v_name in views_to_drop:
                drop_view_sql = text(f'DROP VIEW "{s_name}"."{v_name}";')
                conn.execute(drop_view_sql)
                logger.debug(f"Dropped indexed view: '{s_name}.{v_name}'.")
    except Exception as e:
        logger.error(f"Failed during indexed view dropping phase: {e}")
        raise

def _apply_automatic_date_rules(conn, schema_name):
    """
    Finds all date/datetime columns and applies a MIN YEAR 2000 CHECK constraint.
    This function acts as a "worker" within a larger transaction.
    """
    logger.debug("Applying automatic date constraints (MIN YEAR 2000)...")
    
    # This function now expects to be inside an existing transaction, so we remove its own 'with conn.begin()'
    try:
        date_cols_query = text("""
            SELECT c.TABLE_NAME, c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
            WHERE t.TABLE_TYPE = 'BASE TABLE'
              AND c.TABLE_SCHEMA = :schema
              AND c.DATA_TYPE IN ('date', 'datetime', 'datetime2', 'smalldetime')
              AND c.COLUMN_NAME NOT IN ('last_updated');
        """)
        # Use parameter binding for safety
        date_columns_to_check = conn.execute(date_cols_query, {"schema": schema_name}).fetchall()

        for table, column in date_columns_to_check:
            full_column_path = f'"{schema_name}"."{table}"."{column}"'
            
            find_old_dates_sql = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table}" WHERE "{column}" < \'2000-01-01\';')
            if conn.execute(find_old_dates_sql).scalar() > 0:
                logger.warning(f"Found dates before 2000 in {full_column_path}. Setting them to NULL.")
                clean_sql = text(f'UPDATE "{schema_name}"."{table}" SET "{column}" = NULL WHERE "{column}" < \'2000-01-01\';')
                conn.execute(clean_sql)

            constraint_name = f"CK_{table}_{column}_min_year_2000"
            add_constraint_sql = text(f"""
                ALTER TABLE "{schema_name}"."{table}"
                ADD CONSTRAINT "{constraint_name}" CHECK ("{column}" IS NULL OR "{column}" >= '2000-01-01');
            """)
            try:
                conn.execute(add_constraint_sql)
                logger.debug(f"SUCCESS: Applied MIN YEAR 2000 constraint to {full_column_path}")
            except SQLAlchemyError as e:
                if "already exists" in str(e).lower():
                     logger.debug(f"Constraint '{constraint_name}' already exists on {full_column_path}. This should have been dropped; skipping.")
                else:
                    # Log the warning but re-raise the error to fail the main transaction
                    logger.warning(f"Could not apply date constraint to {full_column_path}. Reason: {e.orig}")
                    raise

        logger.info("Successfully applied all date rules.")

    except Exception as e:
        logger.error(f"A critical error occurred during automatic date rule application. The main transaction will be rolled back. Error: {e}")
        # Re-raise the exception to ensure the controller's 'with' block catches it and rolls back.
        raise

def _clean_data_for_table(conn, schema_name, table, rules):
    """Builds and executes a single bulk UPDATE statement to clean data for one table."""
    update_conditions = defaultdict(list)
    for rule in rules:
        column_name = str(rule['column_name']).strip()
        constraint_type = str(rule['constraint_type']).upper()
        value = rule.get('value')

        if ";" in column_name or pd.isna(value): continue

        if constraint_type == "MAX":
            update_conditions[column_name].append(f'"{column_name}" > {value}')
        elif constraint_type == "MIN":
            update_conditions[column_name].append(f'"{column_name}" < {value}')
    
    if update_conditions:
        set_clauses = []
        for col, conditions in update_conditions.items():
            full_condition = " OR ".join(conditions)
            set_clauses.append(f'"{col}" = CASE WHEN {full_condition} THEN NULL ELSE "{col}" END')
        
        clean_sql = f'UPDATE "{schema_name}"."{table}" SET {", ".join(set_clauses)};'
        logger.debug(f"Executing bulk data cleaning for table '{table}'...")
        conn.execute(text(clean_sql))

def _apply_constraints_for_table(conn, schema_name, table, rules):
    """
    Applies all constraints for a single table. It is now robust against non-deterministic
    data types, single-column unique constraints, and non-existent columns in rules.
    Operations are wrapped in a nested transaction (SAVEPOINT).
    """
    full_table_name = f'"{schema_name}"."{table}"'
    
    transaction = conn.begin_nested()
    try:
        for rule in rules:
            column_name_raw = str(rule['column_name']).strip()
            constraint_type = str(rule['constraint_type']).upper()
            
            if constraint_type in ("MAX", "MIN"):
                value = rule.get('value')
                column_name = column_name_raw.split(';')[0].strip()
                op = "<=" if constraint_type == "MAX" else ">="
                constraint_name = f"CK_{table}_{column_name}_{constraint_type.lower()}"
                
                check_sql = text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" CHECK ("{column_name}" {op} {value})')
                
                try:
                    logger.debug(f"Applying CHECK constraint '{constraint_name}'...")
                    conn.execute(check_sql)
                except SQLAlchemyError as e:
                    logger.warning(f"Could not apply CHECK constraint '{constraint_name}'. Reason: {e.orig}")
                continue

            elif constraint_type == "UNIQUE":
                cols_for_unique = [c.strip() for c in column_name_raw.split(';')]
                
                # --- FIX 3: Validate that all specified columns exist in the table ---
                cols_placeholder = ", ".join([f"'{c}'" for c in cols_for_unique])
                validation_query = text(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table}'
                    AND COLUMN_NAME IN ({cols_placeholder})
                """)
                if conn.execute(validation_query).scalar() != len(cols_for_unique):
                    logger.error(f"Rule skipped: Not all columns for UNIQUE constraint ({', '.join(cols_for_unique)}) exist in table {full_table_name}.")
                    continue # Skip this rule and move to the next

                # --- Build the sanitization and concatenation SQL ---
                sanitized_cols_sql = []
                for col in cols_for_unique:
                    # --- FIX 1: Make hash deterministic ---
                    # Use ISNULL and CONVERT instead of CAST to ensure deterministic string conversion.
                    formula = (
                        "UPPER(TRIM(REPLACE(REPLACE(REPLACE("
                        f"ISNULL(CONVERT(NVARCHAR(4000), \"{col}\", 121), ''), " # Style 121 is ODBC canonical, good for determinism
                        "CHAR(13), ' '), CHAR(10), ' '), CHAR(160), ' ')))"
                    )
                    sanitized_cols_sql.append(formula)
                
                # --- FIX 2: Handle single-column UNIQUE constraints ---
                if len(sanitized_cols_sql) > 1:
                    concat_sql = f"CONCAT_WS('||', {', '.join(sanitized_cols_sql)})"
                else:
                    # If only one column, no need for CONCAT_WS
                    concat_sql = sanitized_cols_sql[0]

                # --- Define names ---
                constraint_name_cols = '_'.join(cols_for_unique)
                hash_column_name = f"uq_hash_{constraint_name_cols}"
                constraint_name = f"UQ_{table}_{constraint_name_cols}"

                # --- Build ADD COLUMN and ADD CONSTRAINT statements ---
                add_col_sql = text(f"""
                ALTER TABLE {full_table_name}
                ADD "{hash_column_name}" AS (
                    HASHBYTES('SHA2_256', {concat_sql})
                ) PERSISTED
                """)

                add_constraint_sql = text(f'ALTER TABLE {full_table_name} ADD CONSTRAINT "{constraint_name}" UNIQUE ("{hash_column_name}")')

                # --- Execute statements ---
                try:
                    logger.debug(f"Creating hash column '{hash_column_name}' on {full_table_name}...")
                    conn.execute(add_col_sql)
                except SQLAlchemyError as e:
                    if "already exists" in str(e.orig):
                        logger.debug(f"Hash column '{hash_column_name}' already exists. Skipping creation.")
                    else:
                        logger.error(f"Failed to create hash column '{hash_column_name}'. Reason: {e.orig}")
                        raise

                try:
                    logger.debug(f"Applying UNIQUE constraint '{constraint_name}'...")
                    conn.execute(add_constraint_sql)
                except SQLAlchemyError as e:
                    logger.warning(f"Could not apply UNIQUE constraint '{constraint_name}'. Reason: {e.orig}")
                    raise

        # If the loop completes, commit the nested transaction
        transaction.commit()
        logger.debug(f"Successfully processed constraints for table {full_table_name}.")

    except Exception as e:
        # If any error occurs, roll back the nested transaction
        logger.error(f"An unexpected error occurred while processing {full_table_name}. Rolling back changes for this table.")
        transaction.rollback()
        raise

def _apply_indexed_view_rules(conn, schema_name, view_rules_df):
    """
    Processes rules of type 'UNIQUE_VIEW' from the dataframe to create unique indexed views.
    This version uses the correct SQL Server syntax for naming columns in a schema-bound view.
    """
    if view_rules_df.empty:
        return
    logger.debug("Applying UNIQUE_VIEW (indexed view) constraints...")
    for _, rule in view_rules_df.iterrows():
        try:
            view_name = rule['table_name']
            columns_str = rule['column_name']
            join_tables_str = rule['join_tables']
            join_condition = rule['join_condition']

            # --- Parse CSV inputs ---
            # 'ps.period;ps.project_name' -> ['period', 'project_name']
            select_cols_unquoted = [c.split(".")[-1].strip() for c in columns_str.split(';')]
            # 'ps.period;ps.project_name' -> '"period", "project_name"'
            index_cols_quoted = [f'"{col}"' for col in select_cols_unquoted]
            
            select_clause = ', '.join(c.strip() for c in columns_str.split(';'))
            
            table_definitions = join_tables_str.split(';')
            table_joins_list = []
            for t_def in table_definitions:
                parts = [p.strip() for p in t_def.split(' AS ')]
                table_name, alias = parts[0], parts[1]
                sql_fragment = f'"{schema_name}"."{table_name}" AS {alias}'
                table_joins_list.append(sql_fragment)
            from_clause_schemabound = ' INNER JOIN '.join(table_joins_list)

            # --- Define Names and Final SQL ---
            full_view_name = f'"{schema_name}"."{view_name}"'
            index_name = f'UX_{view_name}'
            
            # --- FIX: Define all column names for the view in the CREATE VIEW line ---
            # This includes a name for the COUNT_BIG(*) column, e.g., "RecordCount"
            view_column_names = index_cols_quoted + ['"RecordCount"']
            view_def_columns = f"({', '.join(view_column_names)})"

            create_view_sql = text(f"""
                CREATE VIEW {full_view_name} {view_def_columns}
                WITH SCHEMABINDING
                AS
                SELECT
                    {select_clause},
                    COUNT_BIG(*)
                FROM
                    {from_clause_schemabound}
                ON
                    {join_condition}
                GROUP BY
                    {select_clause};
            """)

            create_index_sql = text(f"""
                CREATE UNIQUE CLUSTERED INDEX {index_name}
                ON {full_view_name} ({', '.join(index_cols_quoted)});
            """)

            logger.debug(f"Creating schema-bound view: {view_name}")
            conn.execute(create_view_sql)
            
            logger.debug(f"Creating unique clustered index on view: {index_name}")
            conn.execute(create_index_sql)
            logger.debug(f"SUCCESS: Created indexed view {view_name} to enforce uniqueness.")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create indexed view for rule '{rule['table_name']}'. Reason: {e.orig}")
            raise # Re-raise to ensure transaction rollback

def _run_phase_1_drop_constraints(engine, schema_name: str):
    """
    Phase 1: Connects to the DB, drops all constraints in a single transaction, and commits.
    """
    logger.info("--- Starting Phase 1: Dropping all existing constraints... ---")
    try:
        with engine.begin() as conn:
            _drop_existing_constraints(conn, schema_name)
        logger.info("--- Phase 1 completed successfully and changes have been committed. ---")
        return True
    except Exception as e:
        logger.error(f"--- Phase 1 FAILED. The transaction was rolled back. Error: {e} ---")
        return False


def _run_phase_2_apply_date_rules(engine, schema_name: str):
    """
    Phase 2: Connects, applies automatic date rules in a transaction, and commits.
    """
    logger.info("--- Starting Phase 2: Applying automatic date rules (MIN YEAR 2000)... ---")
    try:
        with engine.begin() as conn:
            _apply_automatic_date_rules(conn, schema_name)
        logger.info("--- Phase 2 completed successfully and changes have been committed. ---")
        return True
    except Exception as e:
        logger.error(f"--- Phase 2 FAILED. The transaction was rolled back. Error: {e} ---")
        return False


def _run_phase_3_apply_csv_rules(engine, schema_name: str, csv_path: str):
    """
    Phase 3: Connects, applies all CSV-based rules in a transaction, and commits.
    """
    logger.info("--- Starting Phase 3: Applying CSV-based constraints... ---")
    try:
        constraints_df = pd.read_csv(csv_path)
        constraints_df.fillna({'join_tables': '', 'join_condition': ''}, inplace=True)
    except FileNotFoundError:
        logger.warning(f"Constraint CSV not found at {csv_path}. Skipping Phase 3.")
        return True # Not a failure, just nothing to do.

    if constraints_df.empty:
        logger.info("No constraints in CSV. Skipping Phase 3.")
        return True

    try:
        with engine.begin() as conn:
            # Separate and apply view rules first
            view_rules_df = constraints_df[constraints_df['constraint_type'] == 'UNIQUE_VIEW'].copy()
            if not view_rules_df.empty:
                logger.info("Applying indexed view rules from CSV...")
                _apply_indexed_view_rules(conn, schema_name, view_rules_df)

            # Apply table-based rules
            table_rules_df = constraints_df[constraints_df['constraint_type'] != 'UNIQUE_VIEW'].copy()
            grouped_constraints = table_rules_df.groupby('table_name')
            
            for table, group_df in grouped_constraints:
                logger.debug(f"Processing table-based CSV rules for table: {table}")
                try:
                    _clean_data_for_table(conn, schema_name, table, group_df.to_dict('records'))
                    _apply_constraints_for_table(conn, schema_name, table, group_df.to_dict('records'))
                except Exception as e_inner:
                    # The use of SAVEPOINT in _apply_constraints_for_table means we can log and continue
                    logger.error(f"Failed processing table '{table}'. Changes for this table rolled back. Continuing... Error: {e_inner}")
                    continue
        
        logger.info("--- Phase 3 completed successfully and changes have been committed. ---")
        return True
    except Exception as e:
        logger.error(f"--- Phase 3 FAILED. The transaction was rolled back. Error: {e} ---")
        return False


def apply_data_quality_rules(target_db_url: str, schema_name: str, csv_path: str):
    """
    Orchestrates the application of data quality rules, committing after each major phase.
    """
    logger.info(f"Starting data quality rule application for schema '{schema_name}'...")
    engine = create_engine(target_db_url)

    # Run Phase 1: Drop Constraints
    if not _run_phase_1_drop_constraints(engine, schema_name):
        logger.critical("Phase 1 (Dropping Constraints) failed. Aborting the rest of the pipeline.")
        return

    # Run Phase 2: Apply Date Rules
    if not _run_phase_2_apply_date_rules(engine, schema_name):
        logger.critical("Phase 2 (Applying Date Rules) failed. Aborting the rest of the pipeline.")
        return

    # Run Phase 3: Apply CSV Rules
    if not _run_phase_3_apply_csv_rules(engine, schema_name, csv_path):
        logger.critical("Phase 3 (Applying CSV Rules) failed.")
        return
    
    logger.info("All phases of data quality rule application completed.")