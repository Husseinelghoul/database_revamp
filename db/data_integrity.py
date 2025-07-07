import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config.constants import PROCESSING_CHUNK_SIZE
from utils.logger import setup_logger
from utils.utils import generate_constraint_name, load_schema_changes

logger = setup_logger()

def add_primary_keys(target_db_url, schema_name):
    """
    Assertively ensures that for every table with an 'id' column, 'id' is the primary key,
    EXCLUDING a predefined list of infrastructure tables.
    """
    engine = create_engine(target_db_url)
    excluded_tables = (
        'app_user', 'user_view_permission', 'user_project', 'user_entity',
        'sessions', 'role_template'
    )
    excluded_tables_sql_str = ", ".join([f"'{table}'" for table in excluded_tables])
    discovery_sql = text(f"""
        SELECT
            t.TABLE_NAME, c.COLUMN_NAME AS IdColumnName, pk.ConstraintName, pk.PkColumnName
        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        LEFT JOIN (
            SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME AS ConstraintName, kcu.COLUMN_NAME AS PkColumnName
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.TABLE_SCHEMA = :schema_name AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) AS pk ON t.TABLE_SCHEMA = pk.TABLE_SCHEMA AND t.TABLE_NAME = pk.TABLE_NAME
        WHERE t.TABLE_SCHEMA = :schema_name
          AND t.TABLE_TYPE = 'BASE TABLE'
          AND LOWER(c.COLUMN_NAME) = 'id'
          AND t.TABLE_NAME NOT IN ({excluded_tables_sql_str});
    """)
    try:
        with engine.connect() as conn:
            results = conn.execute(discovery_sql, {"schema_name": schema_name}).fetchall()
            tables_to_process = [(row[0], row[1], row[2], row[3]) for row in results]
    except SQLAlchemyError as e:
        logger.exception(f"Fatal: Failed to query schema information.")
        return
    if not tables_to_process:
        logger.debug(f"No tables requiring primary key changes were found in schema '{schema_name}' (after exclusions). No action needed.")
        return
    logger.debug(f"Found {len(tables_to_process)} tables that need a primary key on their 'id' column.")
    with engine.begin() as conn:
        for table_name, id_column_name, pk_constraint_name, pk_column_name in tables_to_process:
            full_table_name = f'"{schema_name}"."{table_name}"'
            if pk_constraint_name and pk_column_name.lower() == id_column_name.lower():
                logger.debug(f"OK: Primary key on {full_table_name} is already correctly set to '{id_column_name}'.")
                continue
            try:
                if pk_constraint_name:
                    logger.debug(f"FIXING: {full_table_name} has a PK '{pk_constraint_name}' on the wrong column ('{pk_column_name}'). Dropping it.")
                    conn.execute(text(f'ALTER TABLE {full_table_name} DROP CONSTRAINT "{pk_constraint_name}";'))
                logger.debug(f"ACTION: Adding primary key to {full_table_name} on column '{id_column_name}'...")
                conn.execute(text(f'ALTER TABLE {full_table_name} ALTER COLUMN "{id_column_name}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_table_name} ADD PRIMARY KEY ("{id_column_name}");'))
                logger.debug(f"SUCCESS: Primary key for {full_table_name} is now set on '{id_column_name}'.")
            except SQLAlchemyError:
                logger.exception(f"FAILED: Could not set primary key for {full_table_name}. The 'id' column might contain NULLs or duplicate values. Skipping this table.")


def implement_one_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/one_to_many_relations.csv"):
    """
    Implement one-to-many relations based on CSV configuration.
    It now supports composite lookup keys.
    
    CSV Format: table_name,column_name,replaced_with,referenced_table,referenced_column,lookup_column
    - column_name: The column(s) in the source table. For multiple columns, separate with a hyphen ('-').
    - lookup_column: The column(s) in the referenced table. For multiple columns, separate with a hyphen ('-').
    - The order of columns in 'column_name' and 'lookup_column' must match.
    """
    MULTI_COLUMN_SEPARATOR = '-'
    try:
        relations_df = load_schema_changes(csv_path)
    except Exception as e:
        logger.error(f"Could not load one-to-many relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)
    
    with engine.begin() as conn: 
        for _, row in relations_df.iterrows():
            table_name = str(row["table_name"]).strip()
            source_lookup_cols_str = str(row["column_name"]).strip()
            replaced_with = str(row["replaced_with"]).strip() 
            referenced_table = str(row["referenced_table"]).strip()
            referenced_column = str(row["referenced_column"]).strip() 
            ref_lookup_cols_str = str(row["lookup_column"]).strip() 

            source_columns = [c.strip() for c in source_lookup_cols_str.split(MULTI_COLUMN_SEPARATOR)]
            lookup_columns = [c.strip() for c in ref_lookup_cols_str.split(MULTI_COLUMN_SEPARATOR)]
            
            if len(source_columns) != len(lookup_columns):
                logger.error(
                    f"Mismatched number of lookup columns for table '{table_name}'. "
                    f"Source has {len(source_columns)} ({source_columns}) but "
                    f"Referenced has {len(lookup_columns)} ({lookup_columns}). Skipping."
                )
                continue

            logger.debug(f"Processing one-to-many: {schema_name}.{table_name} using composite key "
                         f"({source_columns} -> {lookup_columns})")
            
            try:
                check_col_exists_query = text(f"""
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table AND COLUMN_NAME = :col
                """)
                col_exists = conn.execute(check_col_exists_query, {"schema": schema_name, "table": table_name, "col": replaced_with}).fetchone()

                if not col_exists:
                    conn.execute(text(f'ALTER TABLE "{schema_name}"."{table_name}" ADD "{replaced_with}" INT NULL'))
                    logger.debug(f"Added column {replaced_with} to {schema_name}.{table_name}")
                else:
                    logger.debug(f"Column {replaced_with} already exists in {schema_name}.{table_name}.")

                join_conditions = " AND ".join([
                    f'src."{sc}" = ref."{lc}"' for sc, lc in zip(source_columns, lookup_columns)
                ])
                
                update_query = text(f"""
                    UPDATE src
                    SET "{replaced_with}" = ref."{referenced_column}"
                    FROM "{schema_name}"."{table_name}" AS src
                    LEFT JOIN "{schema_name}"."{referenced_table}" AS ref
                        ON {join_conditions}
                    WHERE src."{replaced_with}" IS NULL;
                """)
                result = conn.execute(update_query)
                logger.debug(f"Updated {replaced_with} in {schema_name}.{table_name}. Rows affected: {result.rowcount}")
                
                unmatched_check_conditions = " AND ".join([
                    f"({col} IS NOT NULL AND LTRIM(RTRIM(CAST({col} AS VARCHAR(MAX)))) <> '')" for col in source_columns
                ])
                unmatched_query = text(f"""
                    SELECT COUNT(*) AS unmatched_count
                    FROM "{schema_name}"."{table_name}"
                    WHERE "{replaced_with}" IS NULL AND ({unmatched_check_conditions});
                """)
                unmatched_count = conn.execute(unmatched_query).scalar_one_or_none()
                if unmatched_count and unmatched_count > 0:
                    logger.debug(f"{unmatched_count} entries in {schema_name}.{table_name} could not find a match in {schema_name}.{referenced_table}. "
                                 f"{replaced_with} remains NULL for these.")
                
                fk_constraint_name = generate_constraint_name(
                    prefix="FK", name_elements=[table_name, referenced_table, replaced_with]
                )
                check_fk_exists_query = text("""
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = :fk_name
                    AND TABLE_SCHEMA = :schema AND TABLE_NAME = :table;
                """)
                fk_exists = conn.execute(check_fk_exists_query, {"fk_name": fk_constraint_name, "schema": schema_name, "table": table_name}).fetchone()

                if not fk_exists:
                    add_fk_query = text(f"""
                        ALTER TABLE "{schema_name}"."{table_name}"
                        ADD CONSTRAINT "{fk_constraint_name}"
                        FOREIGN KEY ("{replaced_with}") REFERENCES "{schema_name}"."{referenced_table}"("{referenced_column}")
                        ON DELETE NO ACTION ON UPDATE NO ACTION;
                    """)
                    conn.execute(add_fk_query)
                    logger.debug(f"Added foreign key {fk_constraint_name} on {replaced_with} in {schema_name}.{table_name}.")
                else:
                    logger.debug(f"Foreign key {fk_constraint_name} already exists on {schema_name}.{table_name}.")
                
            except Exception as e:
                logger.error(f"Failed for {schema_name}.{table_name} using key {source_lookup_cols_str}: {e}")
                raise 

def implement_many_to_many_relations(target_db_url: str, schema_name: str, csv_path: str = "config/data_integrity_changes/many_to_many_relations.csv"):
    """
    Creates many-to-many relations using batch processing. Now supports composite
    lookup keys where multiple source columns contain delimited values.
    
    CSV Format: ...source_multi_column,lookup_name_column...
    - source_multi_column: Hyphen-separated list of columns in the source table that contain the values.
    - lookup_name_column: Hyphen-separated list of columns in the lookup table to match against.
    - The order of columns in 'source_multi_column' and 'lookup_name_column' must correspond.
    - 'seperator': The character (e.g., ';') used to split values within each source column.
    """
    MULTI_COLUMN_SEPARATOR = '-'
    try:
        relations_df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Could not load relations from {csv_path}. Aborting. Error: {e}")
        return

    engine = create_engine(target_db_url)
    
    for _, row in relations_df.iterrows():
        source_table, source_id_col = row["source_table"], row["source_id_column"]
        lookup_table, lookup_id_col = row["lookup_table"], row["lookup_id_column"]
        assoc_table, assoc_source_col, assoc_lookup_col = row["associative_table"], row["assoc_source_column"], row["assoc_lookup_column"]
        separator = row["seperator"]
        
        source_multi_col_str = row["source_multi_column"]
        lookup_name_col_str = row["lookup_name_column"]
        source_multi_cols = [c.strip() for c in source_multi_col_str.split(MULTI_COLUMN_SEPARATOR)]
        lookup_name_cols = [c.strip() for c in lookup_name_col_str.split(MULTI_COLUMN_SEPARATOR)]

        if len(source_multi_cols) != len(lookup_name_cols):
            logger.error(f"Mismatched number of lookup columns for associative table '{assoc_table}'. "
                         f"Source has {len(source_multi_cols)} ({source_multi_cols}) but "
                         f"Lookup has {len(lookup_name_cols)} ({lookup_name_cols}). Skipping.")
            continue

        full_source_name = f'"{schema_name}"."{source_table}"'
        full_lookup_name = f'"{schema_name}"."{lookup_table}"'
        full_assoc_name = f'"{schema_name}"."{assoc_table}"'
        logger.debug(f"Processing relation for {full_assoc_name} with composite keys...")

        try:
            # === Step 1: Pre-load lookup table into a dictionary with composite keys ===
            logger.debug(f"Caching lookup table: {full_lookup_name}...")
            quoted_lookup_cols = [f'"{c}"' for c in lookup_name_cols]
            lookup_sql = f'SELECT "{lookup_id_col}", {", ".join(quoted_lookup_cols)} FROM {full_lookup_name}'
            lookup_df = pd.read_sql(lookup_sql, engine)
            
            for col in lookup_name_cols:
                lookup_df[col] = lookup_df[col].astype(str).str.strip()
            
            multi_index = pd.MultiIndex.from_frame(lookup_df[lookup_name_cols])
            lookup_dict = pd.Series(lookup_df[lookup_id_col].values, index=multi_index).to_dict()
            logger.debug(f"Cached {len(lookup_dict)} lookup values using {len(lookup_name_cols)}-part composite key.")

            # === Step 2: Setup a fresh associative table ===
            with engine.begin() as conn:
                logger.debug(f"Recreating empty target table: {full_assoc_name}")
                conn.execute(text(f'DROP TABLE IF EXISTS {full_assoc_name};'))
                create_sql = f'CREATE TABLE {full_assoc_name} ("{assoc_source_col}" INT, "{assoc_lookup_col}" INT);'
                conn.execute(text(create_sql))

            # === Step 3: Stream source table and process in chunks ===
            quoted_source_multi_cols = [f'"{c}"' for c in source_multi_cols]
            cols_to_select = [f'"{source_id_col}"'] + quoted_source_multi_cols
            where_clause = " OR ".join([f'{c} IS NOT NULL' for c in quoted_source_multi_cols])
            source_sql = f'SELECT {", ".join(cols_to_select)} FROM {full_source_name} WHERE {where_clause}'
            
            logger.debug(f"Streaming source table {full_source_name} in chunks of {PROCESSING_CHUNK_SIZE}...")
            total_rows_processed = 0
            for source_chunk_df in pd.read_sql(source_sql, engine, chunksize=PROCESSING_CHUNK_SIZE):
                total_rows_processed += len(source_chunk_df)
                logger.debug(f"  Processing source rows chunk... (up to row {total_rows_processed})")

                # === FIX APPLIED HERE ===
                # Convert columns to string before splitting to handle non-string data types.
                for col in source_multi_cols:
                    source_chunk_df[col] = source_chunk_df[col].astype(str).str.split(separator)
                
                source_chunk_df.dropna(subset=source_multi_cols, inplace=True)

                exploded_df = source_chunk_df.explode(source_multi_cols)
                exploded_df.dropna(subset=source_multi_cols, inplace=True)
                
                if exploded_df.empty:
                    continue

                for col in source_multi_cols:
                    exploded_df[col] = exploded_df[col].str.strip()

                multi_index = pd.MultiIndex.from_frame(exploded_df[source_multi_cols])
                exploded_df[assoc_lookup_col] = multi_index.map(lookup_dict)
                
                final_chunk_df = exploded_df.dropna(subset=[assoc_lookup_col])
                final_chunk_df = final_chunk_df[[source_id_col, assoc_lookup_col]]
                final_chunk_df.columns = [assoc_source_col, assoc_lookup_col]
                
                if not final_chunk_df.empty:
                    final_chunk_df.to_sql(name=assoc_table, con=engine, schema=schema_name, if_exists='append', index=False)
            
            # === Step 4: Finalize the table (remove duplicates and add constraints) ===
            logger.debug(f"Finalizing table {full_assoc_name}...")
            with engine.begin() as conn:
                temp_table_name = f"#{assoc_table}_temp_distinct"
                conn.execute(text(f"SELECT DISTINCT * INTO {temp_table_name} FROM {full_assoc_name};"))
                conn.execute(text(f"TRUNCATE TABLE {full_assoc_name};"))
                conn.execute(text(f"INSERT INTO {full_assoc_name} SELECT * FROM {temp_table_name};"))
                conn.execute(text(f"DROP TABLE {temp_table_name};"))

                pk_name = f"PK_{assoc_table}"
                fk_source_name = f"FK_{assoc_table}_{source_table}"
                fk_lookup_name = f"FK_{assoc_table}_{lookup_table}"

                conn.execute(text(f'ALTER TABLE {full_assoc_name} ALTER COLUMN "{assoc_source_col}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ALTER COLUMN "{assoc_lookup_col}" INT NOT NULL;'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{pk_name}" PRIMARY KEY ("{assoc_source_col}", "{assoc_lookup_col}");'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{fk_source_name}" FOREIGN KEY ("{assoc_source_col}") REFERENCES "{schema_name}"."{source_table}"("{source_id_col}");'))
                conn.execute(text(f'ALTER TABLE {full_assoc_name} ADD CONSTRAINT "{fk_lookup_name}" FOREIGN KEY ("{assoc_lookup_col}") REFERENCES "{schema_name}"."{lookup_table}"("{lookup_id_col}");'))

            logger.debug(f"Successfully created and populated {full_assoc_name}.")

        except Exception as e:
            logger.exception(f"An error occurred while processing the relation for {source_table}")