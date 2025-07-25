from abc import ABC, abstractmethod
import pandas as pd

# ==== FileLoader Interface ====
class FileLoader(ABC):
    @abstractmethod
    def load(self, file_path: str) -> pd.DataFrame:
        pass

# ==== CSVLoader ====
class CSVLoader(FileLoader):
    def load(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path)


# ==== JsonLoader ====
class JSONLoader(FileLoader):
    def load(self, file_path: str) -> pd.DataFrame:
        return pd.read_json(file_path)
    
# wrapper
def read_csv(file_path: str) -> pd.DataFrame:
    return CSVLoader().load(file_path)

def read_json(file_path: str) -> pd.DataFrame:
    return JSONLoader().load(file_path)






