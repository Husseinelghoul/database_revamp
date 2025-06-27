from sqlalchemy import VARCHAR, create_engine, inspect, text
from sqlalchemy.dialects.mssql import NVARCHAR, VARCHAR
from sqlalchemy.exc import SQLAlchemyError

from utils.logger import setup_logger

logger = setup_logger()



def create_project_period_indexes(target_db_url: str, schema: str = 'dbo'):
    """
    Connects to a MS SQL Server database, finds tables with 'project_name' and 'period' columns,
    and creates a composite index on (period, project_name).

    Before indexing, it ensures the 'project_name' column is of type VARCHAR(255).
    If the column is any other type, it will attempt to ALTER it.
    This could cause data truncation if existing data exceeds 255 characters.

    The operation is idempotent and safe to run multiple times.

    Args:
        target_db_url (str): The SQLAlchemy connection string for the target MSSQL database.
        schema (str): The database schema to inspect. Defaults to 'dbo'.
    """
    logger.debug(f"Starting index creation process for schema '{schema}'...")

    try:
        engine = create_engine(target_db_url)
        inspector = inspect(engine)
    except ImportError:
        logger.error("Failed to create SQLAlchemy engine. Ensure 'pyodbc' is installed (`pip install pyodbc`).")
        return
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating the engine: {e}")
        return

    table_names = []
    try:
        table_names = inspector.get_table_names(schema=schema)
        logger.debug(f"Found {len(table_names)} tables in '{schema}'. Beginning processing...")
    except SQLAlchemyError as e:
        logger.error(f"Could not connect to the database to inspect tables. Aborting. Error: {e}")
        return

    with engine.connect() as connection:
        for table_name in table_names:
            table_qualified_name = f"[{schema}].[{table_name}]"
            try:
                columns = inspector.get_columns(table_name, schema=schema)
                column_details = {col['name'].lower(): col for col in columns}

                # Ensure both required columns exist before proceeding
                if 'project_name' in column_details and 'period' in column_details:
                    logger.debug(f"Processing {table_qualified_name}: Found required columns.")
                    
                    project_name_col = column_details['project_name']
                    col_type = project_name_col['type']
                    
                    # --- DIRECT LOGIC: Enforce VARCHAR(255) ---
                    # If the column is not already VARCHAR(255), alter it.
                    is_correct_type = (
                        isinstance(col_type, VARCHAR) 
                        and col_type.length == 255
                    )

                    if not is_correct_type:
                        logger.debug(
                            f"Column 'project_name' in {table_qualified_name} is not VARCHAR(255) "
                            f"(current type: {col_type}). Attempting to alter."
                        )
                        # Alter statement now only changes the data type
                        alter_sql = text(f"""
                            ALTER TABLE {table_qualified_name}
                            ALTER COLUMN [project_name] VARCHAR(255);
                        """)
                        try:
                            trans = connection.begin()
                            connection.execute(alter_sql)
                            trans.commit()
                            logger.debug(f"Successfully altered 'project_name' column in {table_qualified_name}.")
                        except SQLAlchemyError as e:
                            logger.error(
                                f"Failed to alter 'project_name' column in {table_qualified_name}. "
                                "This can happen if data exceeds 255 chars. Skipping table. "
                                f"Error: {e}"
                            )
                            if 'trans' in locals() and trans.is_active:
                                trans.rollback()
                            continue # Skip to the next table

                    # --- PROCEED WITH INDEX CREATION ---
                    index_name = f"IX_{table_name}_period_project_name"
                    index_sql = text(f"""
                        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{index_name}' AND object_id = OBJECT_ID('{table_qualified_name}'))
                        BEGIN
                            CREATE NONCLUSTERED INDEX [{index_name}]
                            ON {table_qualified_name} ([period] ASC, [project_name] ASC);
                        END
                    """)
                    
                    logger.debug(f"Executing index creation for {table_qualified_name}...")
                    try:
                        trans = connection.begin()
                        connection.execute(index_sql)
                        trans.commit()
                        logger.debug(f"Successfully created or verified index '[{index_name}]' on {table_qualified_name}.")
                    except SQLAlchemyError as e:
                        logger.error(f"Failed to create index for {table_qualified_name}. Error: {e}")
                        if 'trans' in locals() and trans.is_active:
                            trans.rollback()

            except Exception as e:
                logger.error(f"An unexpected error occurred while processing table {table_qualified_name}. Skipping. Error: {e}")

    logger.debug("Index creation process finished.")
