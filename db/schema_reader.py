from sqlalchemy import create_engine, inspect, MetaData, Table, Column, ForeignKey, Identity
from sqlalchemy.types import String, Integer, Float, Boolean, DateTime
from concurrent.futures import ThreadPoolExecutor
import functools

from utils.utils import get_optimal_thread_count_for_io

def read_schema(source_db_url, schema_name="dbo"):
    engine = create_engine(source_db_url)
    inspector = inspect(engine)
    metadata = MetaData()
    schema = {}
    tables = {}

    type_mapping = {
        "INTEGER": Integer,
        "VARCHAR": String,
        "FLOAT": Float,
        "BOOLEAN": Boolean,
        "DATETIME": DateTime,
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
            col_type = type_mapping.get(str(col["type"]), String)
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
    
    with ThreadPoolExecutor(max_workers=get_optimal_thread_count_for_io()) as executor: #TODO edit max worker
        results = executor.map(process_table, table_names)
    
    for table_name, table_info, table in results:
        schema[table_name] = table_info
        tables[table_name] = table

    return schema, tables