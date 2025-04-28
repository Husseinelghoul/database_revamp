from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Float,
                        ForeignKey, Identity, Integer, MetaData, String, Table,
                        create_engine, inspect)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from utils.logger import setup_logger

logger = setup_logger()

def read_schema(source_db_url, schema_name):
    engine = create_engine(source_db_url)
    inspector = inspect(engine)
    metadata = MetaData()
    schema = {}
    tables = {}

    # Enhanced type mapping for MSSQL
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
        "UNIQUEIDENTIFIER": UNIQUEIDENTIFIER,  # MSSQL-specific type
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
            col_type_str = str(col["type"]).upper()  # Convert type to uppercase for matching
            col_type = type_mapping.get(col_type_str, String)  # Default to String if type is unknown
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

    with ThreadPoolExecutor() as executor:  # Adjust max_workers if needed
        results = executor.map(process_table, table_names)

    for table_name, table_info, table in results:
        schema[table_name] = table_info
        tables[table_name] = table

    return schema, tables


def write_schema(target_db_url, tables, target_schema):
    """
    Writes the schema to the target MSSQL database using dynamically generated SQLAlchemy table objects.
    Uses the schema specified in the configuration.
    Aborts on the first failure by raising an exception.

    :param target_db_url: SQLAlchemy connection URL for the target database.
    :param tables: Dictionary containing dynamically generated SQLAlchemy table objects.
    :param target_schema: Target schema name
    :raises: Exception if any table creation fails.
    """
    engine = create_engine(target_db_url)

    for table_name, table in tables.items():
        try:
            table.schema = target_schema
            table.create(bind=engine, checkfirst=True)
            logger.debug(f"Schema duplication completed for table: {table_name} in schema {target_schema}")
        except Exception as e:
            logger.error(f"Failed to duplicate schema for table: {table_name} in schema {target_schema}. Error: {e}")
            raise e  # Abort the process on the first failure