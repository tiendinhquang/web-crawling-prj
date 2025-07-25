from utils.common.db.connection import DBConnection
from sqlalchemy.engine import Engine
from typing import Optional
import pandas as pd

class DataReader:
    def __init__(self, db_engine: Optional[Engine] = None) -> pd.DataFrame:
        self.db_engine = db_engine or DBConnection().engine

    def get_new_reviews(self):
        q = f"""

        ;WITH reviews AS (
        SELECT display_sku, row_version_number , created_at,row_is_latest ,review_count, LAG(review_count) OVER(PARTITION BY display_sku ORDER BY created_at ASC ) AS last_review_count 
        FROM wayfair.product_skus
        )

        SELECT DISTINCT display_sku,review_count, last_review_count ,review_count - last_review_count AS new_review_count
        FROM reviews 
        WHERE row_is_latest = True AND review_count != last_review_count  



        """
        df = pd.read_sql_query(q, self.db_engine)

        return df
    
reader = DataReader()
print(reader.get_new_reviews())
