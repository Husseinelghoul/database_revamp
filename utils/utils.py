import pandas as pd

from utils.logger import setup_logger

logger = setup_logger()

def write_schema_details_to_file(schema, output_file="schema_details.txt"):
    """
    Writes detailed schema information to a text file.
    :param schema: Dictionary containing schema details.
    :param output_file: Path to the output text file.
    """
    try:
        with open(output_file, "w") as file:
            for table_name, table_info in schema.items():
                file.write(f"Table: {table_name}\n")
                file.write("Columns:\n")
                for column in table_info["columns"]:
                    file.write(f"  - {column['name']} ({column['type']})\n")
                
                file.write("\nDependencies and Constraints:\n")
                
                # Primary Keys
                pks = table_info['primary_keys']['constrained_columns']
                if pks:
                    file.write(f"  Primary Keys: {', '.join(pks)}\n")
                
                # Foreign Keys
                for fk in table_info["foreign_keys"]:
                    constrained = ', '.join(fk['constrained_columns'])
                    referred = f"{fk['referred_table']}.{', '.join(fk['referred_columns'])}"
                    file.write(f"  Foreign Key: {constrained} -> {referred}\n")
                
                # Identity Columns
                identity_columns = [col['name'] for col in table_info["columns"] if col.get('autoincrement', False)]
                if identity_columns:
                    file.write(f"  Identity Columns: {', '.join(identity_columns)}\n")
                
                file.write("\n")
    except Exception as e:
        print(f"Error writing schema details to file: {e}")
        raise  # This will abort the process on the first failure

def load_schema_changes(file_path):
    """Load schema changes from CSV file."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Failed to load schema changes from {file_path}: {e}")
        return pd.DataFrame()
