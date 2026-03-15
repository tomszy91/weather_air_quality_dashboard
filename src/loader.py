'''
Module for saving dataframe as database
'''
import logging
import pandas as pd
import sqlite3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Loader:
    '''
    Save data from Transformer
    '''
    
    def __init__(self, config):
        '''
        Initialize loader with loaded dataframe
        '''         
        self.db_path = config["data"]["database_dir"] + "/weather.db"

    def save_dataframe(self, df, timestamp):
        '''
        Save dataframe to SQLite
        '''
        conn = sqlite3.connect(self.db_path)
        logger.info("Saving dataframe as database")
        
        try:
            existing = pd.read_sql(
                f"SELECT city FROM weather_air_quality WHERE downloaded_utc = '{timestamp}'", 
                conn
                )
                
            if not existing.empty:
                logger.warning(f"Data for {timestamp} already exist, skipping")
                conn.close()
                return
        except pd.io.sql.DatabaseError:
            logger.info(f"Table doesn't exist, creating new one")
             
        df.to_sql("weather_air_quality", conn, if_exists="append", index=False)
        conn.close()
        logger.info(f"Data saved to {self.db_path}")

    
