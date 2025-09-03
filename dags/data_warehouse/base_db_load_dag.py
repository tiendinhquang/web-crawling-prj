
from abc import ABC, abstractmethod
from utils.common.config_manager import get_mapping_config
from utils.common.db_loader.data_loader import DataLoader
import pandas as pd
from airflow.decorators import task
import logging
import os
from datetime import datetime
import csv

class BaseDBLoadDAG(ABC):
    def __init__(self, table_code: str):
        self.table_code = table_code  
        self.table_config = get_mapping_config(table_code)
        self.data_loader = DataLoader()

    @abstractmethod
    def process_data(self):
        """
        Process data before loading to the database
        """
        pass


    @abstractmethod
    def load_data(self, df: pd.DataFrame):
        """
        Load data from the source and save it to the database
        """
        pass
    
    def _build_file_path(self, business_date: datetime, filename: str) -> str:
        """
        Build the file path based on configuration and business date.
        First tries the primary path, then falls back to base_path if it exists.
        
        Args:
            business_date: Business date as datetime object
            filename: Name of the file to create
            
        Returns:
            str: Complete file path
        """
        year, month, day = business_date.year, business_date.month, business_date.day
          
        # Try primary path first (using from_src)
        from_src = self.table_config.get('from_src', 'unknown')
        primary_path = f'data/{from_src}/{year}/{month}/{day}/output/{filename}.csv'
        
        # Check if primary path exists
        if os.path.exists(primary_path):
            return primary_path
        
        # Fallback to base_path if it exists in table_config
        if 'base_path' in self.table_config:
            fallback_path = f'{self.table_config["base_path"].format(year=year, month=month, day=day)}/{filename}.csv'
            if os.path.exists(fallback_path):
                return fallback_path
        
        # Return primary path as default (even if it doesn't exist)
        return primary_path
    
    def _ensure_directory_exists(self, file_path: str) -> None:
        """
        Ensure the directory for the file path exists.
        
        Args:
            file_path: Path to the file
        """
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
    
    def create_dag(self):
        """
        Create a DAG for the data load
        """
        @task
        def init_context(ti):
            """Initialize context with business date and filename"""
            business_date = datetime.now()
            filename = f"{self.table_config['filename']}_{business_date.strftime('%Y-%m-%d')}"
            ti.xcom_push(key="business_date", value=business_date.strftime('%Y-%m-%d'))
            ti.xcom_push(key="filename", value=filename)

        @task
        def process_data(ti):
            logging.info(f"Starting process_data task for {self.table_code}")
            try:
                # Process the data
                df = self.process_data()
                logging.info(f"process_data completed successfully. Shape: {df.shape if df is not None else 'None'}")

                # Lấy lại context từ XCom
                business_date = datetime.strptime(ti.xcom_pull(key="business_date"), "%Y-%m-%d")
                filename = ti.xcom_pull(key="filename")

                file_path = self._build_file_path(business_date, filename)
                self._ensure_directory_exists(file_path)
                df['from_src'] = self.table_config['from_src']
                if not df.empty:
                    df.to_csv(file_path, index=False, quoting=csv.QUOTE_ALL)
                    logging.info(f"Data saved to: {file_path}")
                else:
                    logging.info('df is empty, no data was saved')

            except Exception as e:
                logging.error(f"Error in process_data: {e}")
                raise

        @task
        def load_data_to_db(ti):
            logging.info(f"Starting load_data_to_db task for {self.table_code}")
            try:
                business_date = datetime.strptime(ti.xcom_pull(key="business_date"), "%Y-%m-%d")
                filename = ti.xcom_pull(key="filename")

                file_path = self._build_file_path(business_date, filename)
    
                df = pd.read_csv(file_path)

                self.load_data(df)
                logging.info(f"load_data completed successfully for {self.table_code}")

            except Exception as e:
                logging.error(f"Error in load_data_to_db: {e}")
                raise

        return init_context() >> process_data() >> load_data_to_db()
        













