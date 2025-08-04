from sqlalchemy import create_engine, text

from utils.logger import setup_logger

logger = setup_logger()

def populate_master_roles_for_contract_vo_status(target_db_url, schema_name):
    """
    Adds and populates master role columns in the contract_vo_status table.

    This function connects to a MSSQL database and performs the following actions
    on the `contract_vo_status` table:
    1. Creates three new columns: `master_contractor`, `master_pmc`, and `master_consultant`.
       The data type for these columns is copied from the existing `contractor_name` column.
    2. Populates the new columns based on the value of the `contract_type` column:
       - If `contract_type` is 'Construction', `master_contractor` is set to the `contractor_name`.
       - If `contract_type` is 'Consultancy', `master_consultant` is set to the `contractor_name`.
       - If `contract_type` is 'Supervision', `master_pmc` is set to the `contractor_name`.
    
    Args:
        target_db_url (str): The connection string for the target database.
        schema_name (str): The name of the database schema (e.g., 'dbo').
    """
    table_name = "contract_vo_status"
    source_column = "contractor_name"
    new_columns = ["master_contractor", "master_pmc", "master_consultant"]

    engine = create_engine(target_db_url)

    with engine.begin() as conn:
        try:
            # --- Step 1: Get the data type of the source column ---
            logger.debug(f"Fetching column type for '{source_column}' from table '{schema_name}.{table_name}'.")
            result = conn.execute(text(f"""
                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name AND COLUMN_NAME = :source_column;
            """), {"schema_name": schema_name, "table_name": table_name, "source_column": source_column})
            
            column_info = result.fetchone()
            if not column_info:
                raise ValueError(f"Source column '{source_column}' not found in '{schema_name}.{table_name}'.")

            # --- Step 2: Construct the column definition string ---
            data_type, char_max_length, numeric_precision, numeric_scale = column_info
            
            column_definition = ""
            if data_type.upper() in ["VARCHAR", "NVARCHAR", "CHAR", "NCHAR"]:
                length = "MAX" if char_max_length == -1 else char_max_length
                column_definition = f"{data_type}({length})"
            elif data_type in ["DECIMAL", "NUMERIC"]:
                column_definition = f"{data_type}({numeric_precision}, {numeric_scale})"
            else:
                column_definition = data_type
            
            logger.debug(f"Determined column definition for new columns: {column_definition}")

            # --- Step 3: Add the new columns to the table ---
            for new_column in new_columns:
                try:
                    conn.execute(text(f"""
                        ALTER TABLE {schema_name}.{table_name}
                        ADD {new_column} {column_definition};
                    """))
                    logger.debug(f"Successfully added column '{new_column}' to '{schema_name}.{table_name}'.")
                except Exception as e:
                    # This handles the case where the column might already exist
                    logger.warning(f"Could not add column '{new_column}'. It might already exist. Error: {e}")

            # --- Step 4: Populate the new columns based on contract_type ---
            logger.debug(f"Populating new columns in '{schema_name}.{table_name}' based on 'contract_type'.")
            update_statement = text(f"""
                UPDATE {schema_name}.{table_name}
                SET
                    master_contractor = CASE
                        WHEN contract_type = 'Construction' THEN {source_column}
                        ELSE NULL
                    END,
                    master_consultant = CASE
                        WHEN contract_type = 'Consultancy' THEN {source_column}
                        ELSE NULL
                    END,
                    master_pmc = CASE
                        WHEN contract_type = 'Supervision' THEN {source_column}
                        ELSE NULL
                    END;
            """)
            
            conn.execute(update_statement)
            logger.debug("Data population complete.")

        except Exception as e:
            logger.error(f"An error occurred during the operation on table {schema_name}.{table_name}: {e}")
            # The 'with engine.begin()' block will automatically roll back the transaction on error.


def merge_milestone_status(target_db_url: str, schema_name: str):
    """
    Consolidates the 'status' column into the 'milestone_status' column for
    the 'project_milestone' table.

    Connects to the specified database and performs two actions within a
    single transaction:
    1. Merges non-null values from 'status' into 'milestone_status' using COALESCE.
    2. Drops the now-redundant 'status' column.

    The operation is idempotent; it will not cause an error if the 'status'
    column has already been removed.

    Args:
        target_db_url (str): The connection string for the target MSSQL database.
        schema_name (str): The name of the database schema to operate on.
    """
    table_name = 'project_milestone'
    primary_status_col = 'milestone_status'
    secondary_status_col = 'status'

    logger.debug(
        f"Attempting to merge '{secondary_status_col}' into '{primary_status_col}' "
        f"in table '{schema_name}.{table_name}'."
    )

    try:
        engine = create_engine(target_db_url)
        with engine.begin() as conn:
            # This single SQL block checks for the column's existence before acting.
            # This makes the operation safe to re-run (idempotent).
            merge_and_drop_sql = text(f"""
                IF COL_LENGTH(:schema_name + '.' + :table_name, :secondary_col) IS NOT NULL
                BEGIN
                    -- Step 1: Merge data using COALESCE to prioritize the primary column.
                    UPDATE {schema_name}.{table_name}
                    SET {primary_status_col} = COALESCE({primary_status_col}, {secondary_status_col});

                    -- Step 2: Drop the now-redundant secondary column.
                    ALTER TABLE {schema_name}.{table_name}
                    DROP COLUMN {secondary_status_col};

                    PRINT 'Successfully merged and dropped column {secondary_status_col}.';
                END
                ELSE
                BEGIN
                    PRINT 'Column {secondary_status_col} not found. No action taken.';
                END
            """)

            conn.execute(merge_and_drop_sql, {
                "schema_name": schema_name,
                "table_name": table_name,
                "secondary_col": secondary_status_col
            })

            logger.debug(f"Successfully processed table '{schema_name}.{table_name}'.")

    except Exception as e:
        logger.error(
            f"Failed to consolidate columns in '{schema_name}.{table_name}': {e}"
        )
        # Re-raise the exception to halt execution if this is part of a larger script
        raise