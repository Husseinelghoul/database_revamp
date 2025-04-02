sync_tool/
├── config/
│   ├── __init__.py
│   ├── db_config.py         # Handles database connection details
├── db/
│   ├── __init__.py
│   ├── schema_reader.py     # Reads schema from the source database
│   ├── schema_writer.py     # Writes schema to the target database
│   ├── data_migrator.py     # Migrates data row by row
├── engine/
│   ├── __init__.py
│   ├── sync_engine.py       # Orchestrates the sync process
├── cli/
│   ├── __init__.py
│   ├── cli.py               # CLI interface for triggering the sync
├── utils/
│   ├── __init__.py
│   ├── logger.py            # Logging utilities
│   ├── validator.py         # Validation utilities
├── main.py                  # Entry point for the program