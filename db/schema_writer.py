import sys
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from sqlalchemy import (NVARCHAR, TEXT, BigInteger, Boolean, Column, Date,
                        DateTime, Float, ForeignKey, Identity, Integer,
                        MetaData, String, Table, create_engine, inspect, text)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateSchema, CreateTable

from utils.logger import setup_logger

logger = setup_logger()

@compiles(TEXT, 'mssql')
def compile_text_to_nvarchar_max(type_, compiler, **kw):
    return "NVARCHAR(MAX)"


def replicate_schema_with_sql(source_db_url: str, target_db_url: str, source_schema: str, target_schema: str):
    """
    Replicates a database schema by generating and executing raw SQL CREATE
    TABLE statements, ensuring a perfect 1-to-1 copy.

    The compilation rule above ensures legacy data types are correctly translated.

    :param source_db_url: SQLAlchemy connection URL for the source database.
    :param target_db_url: SQLAlchemy connection URL for the target database.
    :param source_schema: The name of the schema to copy from.
    :param target_schema: The name of the schema to create in the target.
    """
    logger.debug(f"Starting SQL-based schema replication from '{source_schema}' to '{target_schema}'.")

    source_engine = create_engine(source_db_url)
    target_engine = create_engine(target_db_url)
    metadata = MetaData()

    try:
        # Step 1: Read the schema structure accurately using reflection.
        logger.debug(f"Reading schema '{source_schema}' from the source database...")
        metadata.reflect(bind=source_engine, schema=source_schema)
        logger.debug(f"Successfully read {len(metadata.tables)} tables from source.")
    except Exception as e:
        logger.error(f"Failed to read schema from source. Error: {e}")
        raise

    # Step 2: Ensure the target schema exists.
    with target_engine.connect() as connection:
        if not connection.dialect.has_schema(connection, target_schema):
            logger.debug(f"Target schema '{target_schema}' does not exist. Creating it.")
            connection.execute(CreateSchema(target_schema))
            connection.commit()

    # Step 3: Generate and execute a CREATE statement for each table.
    tables_to_create = metadata.sorted_tables
    with target_engine.connect() as connection:
        for table in tables_to_create:
            try:
                if connection.dialect.has_table(connection, table.name, schema=target_schema):
                    logger.debug(f"Table {target_schema}.{table.name} already exists. Skipping.")
                    continue

                table.schema = target_schema
                create_sql = str(CreateTable(table).compile(target_engine)).strip()
                logger.debug(f"Executing DDL for table: {target_schema}.{table.name}\n{create_sql}")
                
                connection.execute(text(create_sql))
                connection.commit()
                logger.debug(f"Successfully created table: {target_schema}.{table.name}")

            except Exception as e:
                logger.error(f"Failed to create table {target_schema}.{table.name}. Error: {e}")
                raise
    
    logger.debug("Schema replication process finished successfully.")
# ==============================================================================
# ## Old Functions (Preserved as Requested) ##
# ==============================================================================

def read_schema(source_db_url, schema_name):
    """
    Original version: Reads schema by manually inspecting and rebuilding each table.
    """
    engine = create_engine(source_db_url)
    inspector = inspect(engine)
    metadata = MetaData()
    schema = {}
    tables = {}

    type_mapping = {
        "INTEGER": Integer,
        "BIGINT": BigInteger,
        "VARCHAR": String,
        "NVARCHAR": String,
        "CHAR": String,
        "TEXT": String,
        "FLOAT": Float,
        "REAL": Float,
        "BOOLEAN": Boolean,
        "BIT": Boolean,
        "DATETIME": DateTime,
        "DATE": Date,
        "UNIQUEIDENTIFIER": UNIQUEIDENTIFIER,
    }

    def process_table(table_name):
        table_info = {
            "columns": inspector.get_columns(table_name, schema=schema_name),
            "primary_keys": inspector.get_pk_constraint(table_name, schema=schema_name),
            "foreign_keys": inspector.get_foreign_keys(table_name, schema=schema_name),
        }

        columns = []
        for col in table_info["columns"]:
            col_name = col["name"]
            col_type_str = str(col["type"]).upper()
            col_type = type_mapping.get(col_type_str, String)
            is_primary = col_name in table_info["primary_keys"]["constrained_columns"]
            is_identity = col.get("autoincrement", False)

            if is_identity:
                columns.append(Column(col_name, col_type, Identity(), primary_key=is_primary))
            else:
                columns.append(Column(col_name, col_type, primary_key=is_primary))

        for fk in table_info["foreign_keys"]:
            fk_column = fk["constrained_columns"][0]
            fk_table = fk["referred_table"]
            fk_referred_column = fk["referred_columns"][0]
            columns.append(Column(fk_column, ForeignKey(f"{fk_table}.{fk_referred_column}")))

        table = Table(table_name, metadata, *columns, schema=schema_name)
        return table_name, table_info, table

    table_names = inspector.get_table_names(schema=schema_name)

    with ThreadPoolExecutor() as executor:
        results = executor.map(process_table, table_names)

    for table_name, table_info, table in results:
        schema[table_name] = table_info
        tables[table_name] = table

    return schema, tables


def write_schema(target_db_url, tables, target_schema):
    """
    Original version: Writes the schema to the target database table by table.
    """
    engine = create_engine(target_db_url)
    inspector = inspect(engine)

    for table_name, table in tables.items():
        try:
            table.schema = target_schema
            full_table_name = f"{target_schema}.{table_name}"

            if not inspector.has_table(table_name, schema=target_schema):
                table.create(bind=engine, checkfirst=True)
                logger.debug(f"Schema duplication completed for table: {table_name} in schema {target_schema}")
            else:
                logger.debug(f"Table {full_table_name} already exists. Skipping creation.")
        except Exception as e:
            logger.error(f"Failed to duplicate schema for table: {table_name} in schema {target_schema}. Error: {e}")
            raise e