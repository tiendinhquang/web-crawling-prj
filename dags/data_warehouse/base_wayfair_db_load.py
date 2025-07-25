
from abc import ABC, abstractmethod
from utils.common.config_manager import get_mapping_config
from utils.common.db_loader.data_loader import DataLoader
import pandas as pd
from airflow.decorators import task

class BaseWayfairDBLoad(ABC):
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
    def get_data(self):
        """
        Get data from the source
        """
        pass

    @abstractmethod
    def load_data(self, df: pd.DataFrame):
        """
        Load data from the source and save it to the database
        """
        pass
    
    def create_dag(self):
        """
        Create a DAG for the data load
        """
        @task
        def process_data():
            df = self.process_data()
            
        @task
        def load_data_to_db():
            df = self.get_data()
            self.load_data(df)
        
        process_data() >> load_data_to_db()













