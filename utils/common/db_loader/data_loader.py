import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Optional, Dict, Any, Union, List
import datetime as dt
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from utils.common.db.connection import DBConnection
from utils.common.config_manager import get_mapping_config
import os
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)


class DataLoaderError(Exception):
    """Custom exception for data loading operations."""
    pass


@dataclass
class ETLConfig:
    """
    Configuration for ETL operations.
    
    Provides default values for metadata columns and source tracking.
    """
    default_schema: str = "public"
    row_start_date: str = "1900-01-01"
    row_end_date: str = "9999-12-31"
    row_is_latest: bool = True
    row_is_delete: bool = False
    row_version_number: int = 1
    from_src: str = ""


class DataLoader(ABC):
    """
    Abstract base class for data loading operations.
    
    Provides a common interface for loading data from different file formats.
    Subclasses must implement the load_data method.
    """
    
    @abstractmethod
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load data from file.
        
        Args:
            file_path: Path to the source file
            
        Returns:
            pd.DataFrame: Loaded data as DataFrame
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass


class CsvDataLoader(DataLoader):
    """
    CSV data loader implementation.
    
    Handles loading data from CSV files with proper error handling.
    """
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load data from CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            pd.DataFrame: Loaded data as DataFrame
            
        Raises:
            DataLoaderError: If file cannot be read or parsed
            ValueError: If file_path is empty or invalid
        """
        if not file_path or not file_path.strip():
            logging.error("File path cannot be empty")
            raise ValueError("File path cannot be empty")
        
        if not os.path.exists(file_path):
            logging.error(f"CSV file not found: {file_path}")
            raise DataLoaderError(f"CSV file not found: {file_path}")
        
        try:
            logging.info(f"Loading CSV data from: {file_path}")
            df = pd.read_csv(file_path)
            logging.info(f"Successfully loaded CSV data: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except pd.errors.EmptyDataError:
            logging.warning(f"CSV file is empty: {file_path}")
            return pd.DataFrame()
        except pd.errors.ParserError as e:
            logging.error(f"Error parsing CSV file {file_path}: {e}")
            raise DataLoaderError(f"Error parsing CSV file {file_path}: {e}")
        except PermissionError as e:
            logging.error(f"Permission denied reading CSV file {file_path}: {e}")
            raise DataLoaderError(f"Permission denied reading CSV file {file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error loading CSV file {file_path}: {e}")
            raise DataLoaderError(f"Unexpected error loading CSV file {file_path}: {e}")


class JsonDataLoader(DataLoader):
    """
    JSON data loader implementation.
    
    Handles loading data from JSON files with proper error handling.
    """
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load data from JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            pd.DataFrame: Loaded data as DataFrame
            
        Raises:
            DataLoaderError: If file cannot be read or parsed
            ValueError: If file_path is empty or invalid
        """
        if not file_path or not file_path.strip():
            logging.error("File path cannot be empty")
            raise ValueError("File path cannot be empty")
        
        if not os.path.exists(file_path):
            logging.error(f"JSON file not found: {file_path}")
            raise DataLoaderError(f"JSON file not found: {file_path}")
        
        try:
            logging.info(f"Loading JSON data from: {file_path}")
            df = pd.read_json(file_path)
            logging.info(f"Successfully loaded JSON data: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except pd.errors.EmptyDataError:
            logging.warning(f"JSON file is empty: {file_path}")
            return pd.DataFrame()
        except ValueError as e:
            logging.error(f"Error parsing JSON file {file_path}: {e}")
            raise DataLoaderError(f"Error parsing JSON file {file_path}: {e}")
        except PermissionError as e:
            logging.error(f"Permission denied reading JSON file {file_path}: {e}")
            raise DataLoaderError(f"Permission denied reading JSON file {file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error loading JSON file {file_path}: {e}")
            raise DataLoaderError(f"Unexpected error loading JSON file {file_path}: {e}")


class ParquetDataLoader(DataLoader):
    """
    Parquet data loader implementation.
    
    Handles loading data from Parquet files with proper error handling.
    """
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load data from Parquet file.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            pd.DataFrame: Loaded data as DataFrame
            
        Raises:
            DataLoaderError: If file cannot be read or parsed
            ValueError: If file_path is empty or invalid
        """
        if not file_path or not file_path.strip():
            logging.error("File path cannot be empty")
            raise ValueError("File path cannot be empty")
        
        if not os.path.exists(file_path):
            logging.error(f"Parquet file not found: {file_path}")
            raise DataLoaderError(f"Parquet file not found: {file_path}")
        
        try:
            logging.info(f"Loading Parquet data from: {file_path}")
            df = pd.read_parquet(file_path)
            logging.info(f"Successfully loaded Parquet data: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logging.error(f"Error loading Parquet file {file_path}: {e}")
            raise DataLoaderError(f"Error loading Parquet file {file_path}: {e}")


class DataLoaderFactory:
    """
    Factory for creating data loaders based on file type.
    
    Provides a centralized way to create appropriate data loaders
    and supports dynamic registration of new loader types.
    """
    
    _loaders = {
        "csv": CsvDataLoader,
        "json": JsonDataLoader,
        "parquet": ParquetDataLoader
    }
    
    @classmethod
    def get_loader(cls, file_type: str) -> DataLoader:
        """
        Factory method to return the right loader class based on file type.

        Args:
            file_type: Type of file to load (csv, json, parquet)
            
        Returns:
            DataLoader: Appropriate data loader instance
            
        Raises:
            ValueError: If file_type is not supported
            
        Note:
            Extensible: You can register new file loaders dynamically.
        """
        if not file_type or not file_type.strip():
            logging.error("File type cannot be empty")
            raise ValueError("File type cannot be empty")
        
        loader_class = cls._loaders.get(file_type.lower())
        if not loader_class:
            supported_types = list(cls._loaders.keys())
            logging.error(f"Unsupported file type: {file_type}. Supported types: {supported_types}")
            raise ValueError(f"Unsupported file type: {file_type}. Supported types: {supported_types}")
        
        logging.debug(f"Created {loader_class.__name__} for file type: {file_type}")
        return loader_class()
    
    @classmethod
    def register_loader(cls, file_type: str, loader_class: type) -> None:
        """
        Register a new data loader.
        
        Args:
            file_type: File type identifier
            loader_class: Class implementing DataLoader interface
            
        Raises:
            ValueError: If file_type is empty or loader_class is invalid
        """
        if not file_type or not file_type.strip():
            logging.error("File type cannot be empty when registering loader")
            raise ValueError("File type cannot be empty when registering loader")
        
        if not issubclass(loader_class, DataLoader):
            logging.error(f"Loader class must inherit from DataLoader, got: {loader_class}")
            raise ValueError(f"Loader class must inherit from DataLoader, got: {loader_class}")
        
        cls._loaders[file_type.lower()] = loader_class
        logging.info(f"Registered new loader: {loader_class.__name__} for file type: {file_type}")


class DataTransformer:
    """
    Handles data transformation operations.
    
    Provides methods for adding metadata, renaming columns, and generating hash keys.
    """
    
    def __init__(self, config: ETLConfig):
        """
        Initialize the data transformer.
        
        Args:
            config: ETL configuration object
        """
        self.config = config
        logging.info("DataTransformer initialized successfully")
    
    def add_metadata_columns(self, df: pd.DataFrame, from_src: str) -> pd.DataFrame:
        """
        Adds standard metadata fields to each row for auditing/versioning purposes.

        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with metadata columns added
            
        Fields added:
            - row_start_date, row_end_date
            - row_is_latest, row_is_delete
            - row_version_number
            - from_src
            - created_at, modified_at
        """
        try:
            if df.empty:
                logging.warning("DataFrame is empty, adding metadata columns to empty DataFrame")
                return df
            
            now = dt.datetime.now()
            
            df['row_start_date'] = self.config.row_start_date
            df['row_end_date'] = self.config.row_end_date
            df['row_is_latest'] = self.config.row_is_latest
            df['row_is_delete'] = self.config.row_is_delete
            df['row_version_number'] = self.config.row_version_number
            df['created_at'] = now
            df['modified_at'] = now
            df['from_src'] = from_src
            
            logging.info(f"Added metadata columns to DataFrame: {len(df)} rows")
            return df
            
        except Exception as e:
            logging.error(f"Error adding metadata columns: {e}")
            raise DataLoaderError(f"Error adding metadata columns: {e}")
    
    def rename_columns(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Rename dataframe columns based on mapping.
        
        Args:
            df: Input DataFrame
            mapping: Dictionary mapping old column names to new names
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns
            
        Raises:
            ValueError: If mapping is invalid
        """
        try:
            if not isinstance(mapping, dict):
                logging.error(f"Mapping must be a dictionary, got: {type(mapping)}")
                raise ValueError(f"Mapping must be a dictionary, got: {type(mapping)}")
            
            if df.empty:
                logging.warning("DataFrame is empty, skipping column renaming")
                return df
            
            # Check if all mapping keys exist in DataFrame
            missing_columns = [col for col in mapping.keys() if col not in df.columns]
            if missing_columns:
                logging.warning(f"Columns not found in DataFrame for renaming: {missing_columns}")
            
            df_renamed = df.rename(columns=mapping)
            logging.info(f"Renamed {len(mapping)} columns in DataFrame")
            return df_renamed
            
        except Exception as e:
            logging.error(f"Error renaming columns: {e}")
            raise DataLoaderError(f"Error renaming columns: {e}")
    
    def add_hash_key(self, df: pd.DataFrame, hash_columns: List[str]) -> pd.DataFrame:
        """
        Add hash key column to dataframe.
        
        Args:
            df: Input DataFrame
            hash_columns: List of column names to use for hash generation
            
        Returns:
            pd.DataFrame: DataFrame with hash_key column added
            
        Raises:
            ValueError: If hash_columns is invalid or columns don't exist
        """
        try:
            if not isinstance(hash_columns, list):
                logging.error(f"Hash columns must be a list, got: {type(hash_columns)}")
                raise ValueError(f"Hash columns must be a list, got: {type(hash_columns)}")
            
            if df.empty:
                logging.warning("DataFrame is empty, adding hash key to empty DataFrame")
                df['hash_key'] = ''
                return df
            
            # Check if all hash columns exist
            missing_columns = [col for col in hash_columns if col not in df.columns]
            if missing_columns:
                logging.error(f"Hash columns not found in DataFrame: {missing_columns}")
                raise ValueError(f"Hash columns not found in DataFrame: {missing_columns}")
            
            def _generate_md5_hash(row, columns):
                """Generate MD5 hash for a row"""
                import hashlib
                raw_string = '_'.join([str(row[col]) for col in columns])
                return hashlib.md5(raw_string.encode()).hexdigest()
            
            df['hash_key'] = df.apply(
                lambda row: _generate_md5_hash(row, columns=hash_columns), 
                axis=1
            )
        
            
            logging.info(f"Added hash key column using {len(hash_columns)} columns")
            return df
            
        except Exception as e:
            logging.error(f"Error adding hash key: {e}")
            raise DataLoaderError(f"Error adding hash key: {e}")


class DatabaseWriter:
    """
    Handles database writing operations.
    
    Provides methods for truncating tables and writing DataFrames to database.
    """
    
    def __init__(self, db_engine: Optional[Engine] = None):
        """
        Initialize the database writer.
        
        Args:
            db_engine: SQLAlchemy engine instance (optional)
        """
        try:
            self.db_engine = db_engine or DBConnection().engine
            logging.info("DatabaseWriter initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize DatabaseWriter: {e}")
            raise DataLoaderError(f"Failed to initialize DatabaseWriter: {e}")
    
    def truncate_table(self, schema: str, table: str) -> None:
        """
        Truncate table and restart identity.
        
        Args:
            schema: Database schema name
            table: Table name
            
        Raises:
            ValueError: If schema or table is empty
            DataLoaderError: If truncation fails
        """
        if not schema or not schema.strip():
            logging.error("Schema name cannot be empty")
            raise ValueError("Schema name cannot be empty")
        
        if not table or not table.strip():
            logging.error("Table name cannot be empty")
            raise ValueError("Table name cannot be empty")
        
        try:
            logging.info(f"Truncating table: {schema}.{table}")
            with self.db_engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE'))
            logging.info(f"Successfully truncated table: {schema}.{table}")
            
        except Exception as e:
            logging.error(f"Error truncating table {schema}.{table}: {e}")
            raise DataLoaderError(f"Error truncating table {schema}.{table}: {e}")
    
    def write_dataframe(self, df: pd.DataFrame, schema: str, table: str) -> None:
        """
        Appends a DataFrame to the target table in the database using SQLAlchemy.

        Args:
            df: DataFrame to write
            schema: Database schema name
            table: Table name
            
        Raises:
            ValueError: If schema or table is empty
            DataLoaderError: If writing fails
            
        Note:
            Use `to_sql()` with `if_exists='append'`.
            Logs the number of rows inserted.
        """
        if not schema or not schema.strip():
            logging.error("Schema name cannot be empty")
            raise ValueError("Schema name cannot be empty")
        
        if not table or not table.strip():
            logging.error("Table name cannot be empty")
            raise ValueError("Table name cannot be empty")
        
        try:
            if df.empty:
                logging.warning(f"DataFrame is empty, skipping write to {schema}.{table}")
                return
            
            logging.info(f"Writing {len(df)} rows to {schema}.{table}")
            df.to_sql(
                table, 
                self.db_engine, 
                schema=schema, 
                if_exists='append', 
                index=False
            )
            logging.info(f"✅ Successfully inserted {len(df)} rows into {schema}.{table}")
            
        except Exception as e:
            logging.error(f"Error writing DataFrame to {schema}.{table}: {e}")
            raise DataLoaderError(f"Error writing DataFrame to {schema}.{table}: {e}")


class UniversalDataLoader:
    """
    Combines file loading, data transformation, and writing to database in a unified ETL interface.

    Supports both full and incremental load modes using mapping configuration.
    """
    
    def __init__(self, db_engine: Optional[Engine] = None, config: Optional[ETLConfig] = None, default_chunk_size: int = 10000):
        """
        Initialize the universal data loader.
        
        Args:
            db_engine: SQLAlchemy engine instance (optional)
            config: ETL configuration object (optional)
            default_chunk_size: Default chunk size for processing large DataFrames (default: 10000)
        """
        try:
            self.db_engine = db_engine or DBConnection().engine
            self.config = config or ETLConfig()
            self.default_chunk_size = default_chunk_size
            self.transformer = DataTransformer(self.config)
            self.writer = DatabaseWriter(self.db_engine)
            logging.info("UniversalDataLoader initialized successfully with default chunk size: {default_chunk_size:,}")
        except Exception as e:
            logging.error(f"Failed to initialize UniversalDataLoader: {e}")
            raise DataLoaderError(f"Failed to initialize UniversalDataLoader: {e}")
    
    def load_data_from_file(self, file_path: str, file_type: str = "csv") -> pd.DataFrame:
        """
        Loads data from a file using the appropriate DataLoader (CSV, JSON, Parquet).

        Args:
            file_path: Path to the source file
            file_type: File type, e.g., 'csv', 'json', 'parquet'

        Returns:
            pd.DataFrame: Loaded data as DataFrame
            
        Raises:
            DataLoaderError: If loading fails
            ValueError: If parameters are invalid
        """
        try:
            if not file_path or not file_path.strip():
                logging.error("File path cannot be empty")
                raise ValueError("File path cannot be empty")
            
            loader = DataLoaderFactory.get_loader(file_type)
            return loader.load_data(file_path)
            
        except DataLoaderError:
            # Re-raise DataLoaderError from loader
            raise
        except Exception as e:
            logging.error(f"Unexpected error loading data from file {file_path}: {e}")
            raise DataLoaderError(f"Unexpected error loading data from file {file_path}: {e}")
    
    def fload_to_db(self, df: pd.DataFrame, table_code: str) -> None:
        """
        Performs a full load to the target database table.

        Args:
            df: DataFrame to load
            table_code: Configuration key for table mapping
            
        Raises:
            DataLoaderError: If loading fails
            ValueError: If table_code is empty or configuration not found
            
        Steps:
            - Get mapping config from table_code.
            - Select only required columns.
            - Generate hash key for each row.
            - Rename columns as per mapping.
            - Add standard metadata fields.
            - Truncate the target table before inserting.
            - Insert new data using SQLAlchemy.

        Use when:
            - Replacing all existing data.
            - There's no need for versioning or row history.
        """
        try:
            if not table_code or not table_code.strip():
                logging.error("Table code cannot be empty")
                raise ValueError("Table code cannot be empty")
            
            if df.empty:
                logging.warning("DataFrame is empty, skipping full load")
                return
            
            table_cfg = get_mapping_config(table_code)
            if not table_cfg:                               
                logging.error(f"Table configuration not found for: {table_code}")
                raise ValueError(f"Table configuration not found for: {table_code}")
            
            logging.info(f"Starting full load for table code: {table_code}")
            
            # Select only required columns
            if 'cols_to_insert' in table_cfg:
                df = df[table_cfg['cols_to_insert'] + table_cfg['etl_cols']]
                logging.info(f"Selected {len(table_cfg['cols_to_insert'])} columns for insertion")

            
            # Rename columns``
            df = self.transformer.rename_columns(df, table_cfg['mapping_cols'])
            # Add hash key
            df = self.transformer.add_hash_key(df, table_cfg['hash_cols'])
            # Get table details
            schema = table_cfg["des_schema"]
            table = table_cfg["des_table"]
            
            # Add metadata columns
            df = self.transformer.add_metadata_columns(df, table_cfg['from_src'])
            
            # Truncate table before insert
            self.writer.truncate_table(schema, table)
            
            # Write to database
            self.writer.write_dataframe(df, schema, table)
            
            logging.info(f"✅ Full load completed successfully for {table_code}")
            
        except DataLoaderError:
            # Re-raise DataLoaderError from other methods
            raise
        except Exception as e:
            logging.error(f"Unexpected error in full load for {table_code}: {e}")
            raise DataLoaderError(f"Unexpected error in full load for {table_code}: {e}")
    
    def iload_to_db(self, table_code: str, tmp_table: str, df: pd.DataFrame, chunk_size: Optional[int] = None) -> None:
        """
        Performs incremental load with row versioning and change detection.

        Args:
            table_code: Configuration key for table mapping
            tmp_table: Name of temporary table for staging
            df: DataFrame to load
            chunk_size: Number of rows to process in each chunk (uses default_chunk_size if None)
            
        Raises:
            DataLoaderError: If loading fails
            ValueError: If parameters are invalid
            
        Steps:
            - Get mapping config for the target table.
            - Add hash key to detect changes.
            - Rename and enrich columns with metadata.
            - Process DataFrame in chunks to avoid memory issues.
            - Create a temporary table and insert incoming records.
            - Compare temp vs target table by primary key and hash:
                - If hash differs → update `row_is_latest`, `row_end_date`, insert new version.
                - If not matched → insert new row.
                - If matched with older version → insert current as latest.

        Use when:
            - You want to track historical changes.
            - Handling upserts with audit/version control.
            - Processing large DataFrames that might cause memory issues.
        """
        try:
            if not table_code or not table_code.strip():
                logging.error("Table code cannot be empty")
                raise ValueError("Table code cannot be empty")
            
            if not tmp_table or not tmp_table.strip():
                logging.error("Temporary table name cannot be empty")
                raise ValueError("Temporary table name cannot be empty")
            
            if df.empty:
                logging.warning("DataFrame is empty, skipping incremental load")
                return
            
            table_cfg = get_mapping_config(table_code)
            if not table_cfg:
                logging.error(f"Table configuration not found for: {table_code}")
                raise ValueError(f"Table configuration not found for: {table_code}")
            
            logging.info(f"Starting incremental load for table code: {table_code}")
            
            # Select only required columns
            if 'cols_to_insert' in table_cfg:
                df = df[table_cfg['cols_to_insert']]
                logging.info(f"Selected {len(table_cfg['cols_to_insert'])} columns for insertion")
            
            # Add hash key
            df = self.transformer.add_hash_key(df, table_cfg['hash_cols'])
            
            # Rename columns
            df = self.transformer.rename_columns(df, table_cfg['mapping_cols'])
            
            # Add metadata columns
            df = self.transformer.add_metadata_columns(df, table_cfg['from_src'])
            
            # Get table details
            schema = table_cfg["des_schema"]
            table = table_cfg["des_table"]
            des_cols = df.columns.tolist()
            now = dt.datetime.now()

            logging.info(f"🔄 Processing {schema}.{table}, number of rows: {len(df)}")
            logging.info(f"Columns to be inserted: {des_cols}")
            if 'from_src' in des_cols:
                logging.info(f"from_src values in DataFrame: {df['from_src'].unique()}")
            
            # Use default chunk size if none provided
            if chunk_size is None:
                chunk_size = self.default_chunk_size
            
            # Process DataFrame in chunks if it's large
            total_rows = len(df)
            if total_rows > chunk_size:
                logging.info(f"📦 Large DataFrame detected ({total_rows:,} rows). Processing in chunks of {chunk_size:,}")
                chunks_processed = 0
                
                for start_idx in range(0, total_rows, chunk_size):
                    end_idx = min(start_idx + chunk_size, total_rows)
                    chunk_df = df.iloc[start_idx:end_idx].copy()
                    chunks_processed += 1
                    
                    logging.info(f"🔄 Processing chunk {chunks_processed}/{(total_rows + chunk_size - 1) // chunk_size} "
                               f"({start_idx + 1:,} to {end_idx:,} of {total_rows:,} rows)")
                    
                    self._process_chunk_upsert(chunk_df, table_cfg, tmp_table, schema, table, des_cols, now)
                    
                logging.info(f"✅ All {chunks_processed} chunks processed successfully")
            else:
                logging.info(f"📦 Processing DataFrame as single chunk ({total_rows:,} rows)")
                self._process_chunk_upsert(df, table_cfg, tmp_table, schema, table, des_cols, now)
            
            logging.info(f"✅ Incremental load completed successfully for {table_code}")
            
        except DataLoaderError:
            # Re-raise DataLoaderError from other methods
            raise
        except Exception as e:
            logging.error(f"Unexpected error in incremental load for {table_code}: {e}")
            raise DataLoaderError(f"Unexpected error in incremental load for {table_code}: {e}")
    
    def _process_chunk_upsert(self, chunk_df: pd.DataFrame, table_cfg: Dict[str, Any], tmp_table: str, 
                       schema: str, table: str, des_cols: List[str], now: dt.datetime) -> None:
        """
        Process a single chunk of data for incremental load.
        
        Args:
            chunk_df: DataFrame chunk to process
            table_cfg: Table configuration dictionary
            tmp_table: Temporary table name
            schema: Database schema name
            table: Table name
            des_cols: Destination columns list
            now: Current timestamp
            
        Raises:
            DataLoaderError: If processing fails
        """
        try:
            # Create temporary table
            create_tmp_table_q = f"""
                CREATE TEMPORARY TABLE {tmp_table} AS
                SELECT * FROM {schema}.{table}
                WHERE 1 = 0
            """
            # Drop temporary table if it exists
            drop_tmp_table_q = f"DROP TABLE IF EXISTS {tmp_table}"
            # Update row version for changed records
            update_row_version_q = f"""
                UPDATE {tmp_table} t1
                SET row_version_number = t2.row_version_number + 1,
                row_start_date = '{now}'
                FROM {schema}.{table} t2
                WHERE ({" AND ".join([f"t1.{col} = t2.{col}" for col in table_cfg['primary_key']])}) 
                AND t1.hash_key != t2.hash_key AND t2.row_is_latest = TRUE
            """
            
            # Main merge operation
            main_table_q = f"""
                MERGE INTO {schema}.{table} AS target
                USING {tmp_table} AS source
                ON {" AND ".join([f"target.{col} = source.{col}" for col in table_cfg['primary_key']])}
                WHEN MATCHED AND source.hash_key != target.hash_key AND target.row_is_latest = TRUE THEN
                    UPDATE SET
                        row_end_date = '{now}',
                        row_is_latest = FALSE,
                        modified_at = source.modified_at
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(des_cols)})
                    VALUES ({', '.join(['source.' + col for col in des_cols])});

                INSERT INTO {schema}.{table}({', '.join(des_cols)})
                SELECT {', '.join(['source.' + col for col in des_cols])}
                FROM {tmp_table} source
                JOIN {schema}.{table} target
                ON {" AND ".join([f"source.{col} = target.{col}" for col in table_cfg['primary_key']])}
                WHERE source.hash_key != target.hash_key AND target.row_is_latest = FALSE AND target.row_end_date = '{now}'
            """

            with self.db_engine.begin() as conn:
                conn.execute(text(create_tmp_table_q))
                chunk_df.to_sql(tmp_table, conn, index=False, if_exists='append', method='multi')
                conn.execute(text(update_row_version_q))
                conn.execute(text(main_table_q))
                conn.execute(text(drop_tmp_table_q))
            logging.info(f"✅ Chunk processed successfully: {len(chunk_df):,} rows")
            
        except Exception as e:
            logging.error(f"Error processing chunk: {e}")
            raise DataLoaderError(f"Error processing chunk: {e}")
    
    def load_data_to_db(
        self, 
        df: pd.DataFrame, 
        table_key: str, 
        db_engine: Optional[Engine] = None,
        load_type: str = "full",
        chunk_size: Optional[int] = None
    ) -> None:
        """
        Main entry point for loading data to the database.

        Automatically chooses between full or incremental load based on `load_type`.

        Args:
            df: Data to load
            table_key: Key to retrieve table mapping config
            db_engine: Optional database engine
            load_type: 'full' (truncate and reload) or 'incremental' (upsert with history)
            chunk_size: Number of rows to process in each chunk for incremental loads (default: 10000)
            
        Raises:
            DataLoaderError: If loading fails
            ValueError: If parameters are invalid
        """
        try:
            if not table_key or not table_key.strip():
                logging.error("Table key cannot be empty")
                raise ValueError("Table key cannot be empty")
            
            if load_type not in ["full", "incremental"]:
                logging.error(f"Invalid load type: {load_type}. Must be 'full' or 'incremental'")
                raise ValueError(f"Invalid load type: {load_type}. Must be 'full' or 'incremental'")
            
            # Use provided engine or create new one
            if db_engine:
                self.db_engine = db_engine
                self.writer = DatabaseWriter(db_engine)
                logging.info("Using provided database engine")
            
            logging.info(f"Starting {load_type} load for table key: {table_key}")
            
            if load_type.lower() == "incremental":
                # For incremental load, we need a temporary table name
                tmp_table = f"tmp_{table_key.replace('.', '_')}"
                self.iload_to_db(table_key, tmp_table, df, chunk_size)
            else:
                # Full load
                self.fload_to_db(df, table_key)
                
        except DataLoaderError:
            # Re-raise DataLoaderError from other methods
            raise
        except Exception as e:
            logging.error(f"Unexpected error in load_data_to_db for {table_key}: {e}")
            raise DataLoaderError(f"Unexpected error in load_data_to_db for {table_key}: {e}")


# Backward compatibility - maintain the old DataLoader class
class DataLoader:
    """
    Legacy DataLoader class for backward compatibility.
    
    Wraps UniversalDataLoader to maintain existing API.
    """
    
    def __init__(self, db_engine: Optional[Engine] = None):
        """
        Initialize the legacy DataLoader.
        
        Args:
            db_engine: SQLAlchemy engine instance (optional)
        """
        try:
            self.universal_loader = UniversalDataLoader(db_engine)
            logging.info("Legacy DataLoader initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize legacy DataLoader: {e}")
            raise DataLoaderError(f"Failed to initialize legacy DataLoader: {e}")

    def fload_to_db(self, df: pd.DataFrame, table_code: str) -> None:
        """
        Full load to database - backward compatibility.
        
        Args:
            df: DataFrame to load
            table_code: Configuration key for table mapping
        """
        self.universal_loader.fload_to_db(df, table_code)
    
    def iload_to_db(self, table_code: str, tmp_table: str, df: pd.DataFrame, chunk_size: Optional[int] = None) -> None:
        """
        Incremental load to database - backward compatibility.
        
        Args:
            table_code: Configuration key for table mapping
            tmp_table: Name of temporary table for staging
            df: DataFrame to load
            chunk_size: Number of rows to process in each chunk (uses default_chunk_size if None)
        """
        self.universal_loader.iload_to_db(table_code, tmp_table, df, chunk_size)


# Global instances for convenience
_default_config = ETLConfig()
_default_loader = UniversalDataLoader()


# Convenience functions for backward compatibility
def load_data_from_file(file_path: str, file_type: str = "csv") -> pd.DataFrame:
    """
    Load data from file - convenience function.
    
    Args:
        file_path: Path to the source file
        file_type: File type (csv, json, parquet)
        
    Returns:
        pd.DataFrame: Loaded data as DataFrame
        
    Raises:
        DataLoaderError: If loading fails
    """
    return _default_loader.load_data_from_file(file_path, file_type)


def fload_data_to_db(df: pd.DataFrame, table_key: str, db_engine: Optional[Engine] = None) -> None:
    """
    Full load data to database - convenience function.
    
    Args:
        df: DataFrame to load
        table_key: Configuration key for table mapping
        db_engine: Optional database engine
        
    Raises:
        DataLoaderError: If loading fails
    """
    loader = UniversalDataLoader(db_engine)
    loader.fload_to_db(df, table_key)


def iload_data_to_db(table_key: str, tmp_table: str, df: pd.DataFrame, db_engine: Optional[Engine] = None, chunk_size: Optional[int] = None) -> None:
    """
    Incremental load data to database - convenience function.
    
    Args:
        table_key: Configuration key for table mapping
        tmp_table: Name of temporary table for staging
        df: DataFrame to load
        db_engine: Optional database engine
        chunk_size: Number of rows to process in each chunk (uses default_chunk_size if None)
        
    Raises:
        DataLoaderError: If loading fails
    """
    loader = UniversalDataLoader(db_engine)
    loader.iload_to_db(table_key, tmp_table, df, chunk_size)
















    