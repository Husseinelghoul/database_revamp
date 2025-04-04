import os

import psutil


def get_optimal_thread_count_for_io():
    cpu_count = os.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)  # Convert to GB
    
    # For I/O bound tasks, we can use more threads than CPU cores
    thread_count = cpu_count * 4  # Start with 4 threads per core
    
    # Adjust based on available memory (assuming 0.5GB per thread for I/O tasks)
    max_threads_memory = int(memory_gb / 0.5)
    thread_count = min(thread_count, max_threads_memory)
    
    # For I/O bound tasks, we can allow a higher maximum
    thread_count = min(thread_count, 64)
    
    return max(8, thread_count)  # Ensure at least 8 threads for I/O tasks


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

