import hashlib

import pandas as pd
from sqlalchemy import text

from utils.logger import setup_logger

logger = setup_logger()

def write_schema_details_to_file(schema, output_file="schema_details.txt"):
    """
    Writes detailed schema debugrmation to a text file.
    :param schema: Dictionary containing schema details.
    :param output_file: Path to the output text file.
    """
    try:
        with open(output_file, "w") as file:
            for table_name, table_debug in schema.items():
                file.write(f"Table: {table_name}\n")
                file.write("Columns:\n")
                for column in table_debug["columns"]:
                    file.write(f"  - {column['name']} ({column['type']})\n")
                
                file.write("\nDependencies and Constraints:\n")
                
                # Primary Keys
                pks = table_debug['primary_keys']['constrained_columns']
                if pks:
                    file.write(f"  Primary Keys: {', '.join(pks)}\n")
                
                # Foreign Keys
                for fk in table_debug["foreign_keys"]:
                    constrained = ', '.join(fk['constrained_columns'])
                    referred = f"{fk['referred_table']}.{', '.join(fk['referred_columns'])}"
                    file.write(f"  Foreign Key: {constrained} -> {referred}\n")
                
                # Identity Columns
                identity_columns = [col['name'] for col in table_debug["columns"] if col.get('autoincrement', False)]
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


def generate_constraint_name(prefix: str, name_elements: list, max_length: int = 128) -> str:
    """
    Generates a constraint name, ensuring it's within max_length.
    If the ideal descriptive name is too long, it's truncated and a hash is appended.
    """
    # Clean elements: convert to string, replace spaces, filter out None/empty
    cleaned_elements = [str(el).replace(" ", "_") for el in name_elements if el and str(el).strip()]
    
    if not cleaned_elements: # Should not happen if elements are always provided
        base_name_candidate = "default_constraint"
    else:
        base_name_candidate = "_".join(cleaned_elements)
    
    ideal_name = f"{prefix}_{base_name_candidate}"

    if len(ideal_name) <= max_length:
        return ideal_name

    # Name is too long, create a hashed version. Hash the original ideal name for consistency.
    hasher = hashlib.sha256(ideal_name.encode('utf-8'))
    hash_suffix = hasher.hexdigest()[:8] # 8-character hash

    # Available length for the descriptive part (prefix_elements_truncated) before adding _hash_suffix
    # Structure: prefix_ELEMENTS_hash
    len_prefix_with_underscore = len(prefix) + 1 # e.g., "FK_"
    len_hash_with_underscore = len(hash_suffix) + 1 # e.g., "_abc12345"
    
    available_for_elements_part = max_length - len_prefix_with_underscore - len_hash_with_underscore

    if available_for_elements_part < 10: # Ensure at least some descriptive part
        # Fallback: prefix + first few elements (if any) + hash, truncated if necessary
        elements_part = "_".join(cleaned_elements[:1]) # Take first element or empty if none
        short_name = f"{prefix}_{elements_part}_{hash_suffix}"
        return short_name[:max_length]

    truncated_elements_part = base_name_candidate[:available_for_elements_part]
    # Remove trailing underscore if truncation resulted in one and it wasn't the only char
    if truncated_elements_part.endswith("_") and len(truncated_elements_part) > 1:
        truncated_elements_part = truncated_elements_part[:-1]
        
    final_name = f"{prefix}_{truncated_elements_part}_{hash_suffix}"
    
    # Final check due to underscore handling, truncate if somehow still over
    return final_name[:max_length]


def clean_column_for_datetime_conversion(conn, schema_name: str, table_name: str, column_name: str):
    """
    Performs standard cleaning on a column before attempting date/datetime conversion or analysis.
    - Nullifies 'test'/'lorem' entries.
    - Nullifies empty/whitespace strings.
    - Nullifies entries that cannot be broadly converted to DATETIME2 (as a general pre-check).
    """
    logger.debug(f"Performing standard datetime pre-cleaning for {schema_name}.{table_name}.{column_name}")

    # Step 1: Nullify based on keywords like 'test' or 'lorem' (case-insensitive)
    clean_keywords_sql = f"""
        UPDATE {schema_name}.{table_name}
        SET {column_name} = NULL
        WHERE ({column_name} IS NOT NULL) 
          AND (LOWER(CAST({column_name} AS VARCHAR(MAX))) LIKE '%test%' OR LOWER(CAST({column_name} AS VARCHAR(MAX))) LIKE '%lorem%');
    """
    conn.execute(text(clean_keywords_sql))
    logger.debug(f"Nullified 'test'/'lorem' in {schema_name}.{table_name}.{column_name}.")

    # Step 2: Convert empty strings or strings with only spaces to NULL
    clean_empty_sql = f"""
        UPDATE {schema_name}.{table_name}
        SET {column_name} = NULL
        WHERE ({column_name} IS NOT NULL) AND (LTRIM(RTRIM(CAST({column_name} AS VARCHAR(MAX)))) = '');
    """
    conn.execute(text(clean_empty_sql))
    logger.debug(f"Nullified empty/whitespace strings in {schema_name}.{table_name}.{column_name}.")

    # Step 3: For remaining non-NULL values, set to NULL if they can't even be TRY_CONVERTed to DATETIME2.
    # This is a general pre-filter before specific type analysis or conversion.
    pre_clean_non_convertible_sql = f"""
        UPDATE {schema_name}.{table_name}
        SET {column_name} = NULL
        WHERE {column_name} IS NOT NULL AND TRY_CONVERT(DATETIME2, {column_name}) IS NULL;
    """
    conn.execute(text(pre_clean_non_convertible_sql))
    logger.debug(f"Nullified general non-convertible datetime strings in {schema_name}.{table_name}.{column_name} using DATETIME2 for pre-check.")

