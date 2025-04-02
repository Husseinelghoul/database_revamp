import os
import psutil

def get_optimal_thread_count():
    cpu_count = os.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)  # Convert to GB
    
    # Base thread count on CPU cores
    thread_count = cpu_count * 2  # Start with 2 threads per core
    
    # Adjust based on available memory (assuming 1GB per thread as a rough estimate)
    max_threads_memory = int(memory_gb / 1)
    thread_count = min(thread_count, max_threads_memory)
    
    # Cap at a reasonable maximum, e.g., 32
    thread_count = min(thread_count, 32)
    
    return max(1, thread_count)  # Ensure at least 1 thread