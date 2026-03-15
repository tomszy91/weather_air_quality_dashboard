'''
Module for transforming list data
'''
import logging
import pandas as pd
import numpy as np
import datetime as dt
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Transformer:
    '''
    Transform data from Downloader
    '''
    
    def __init__(self):
        '''
        Initialize transformer with loaded lists
        '''         
        pass

    def weather_data(self, weather_data_raw):
        '''
        Transforms weather_data dataframe
        '''
        logger.info("Transforming weather data")
    
        df = pd.DataFrame(weather_data_raw)
        mask = (df == df.columns).all(axis=1)
        df = df[~mask]
        df["unix_datetime"] = pd.to_datetime(df["unix_datetime"].astype(int), unit="s", utc=True)
        df = df.rename(columns={"unix_datetime":"weather_last_measurement_utc"})
        
        logger.info(f"Transforming finished with success, created DataFrame shape: {df.shape[0]}, {df.shape[1]}")
        
        return df
    
    def pm25_data(self, pm25_data_raw):
        '''
        Transforms pm25_data dataframe
        '''
        logger.info("Transforming PM2.5 data")
        
        df = pd.DataFrame(pm25_data_raw)
        df = df.rename(columns={"sensor_id":"pm25_sensor_id", "datetimeLast.utc":"pm25_last_measurement_utc", "latest.value":"pm25_last_value[ug/m3]"})
        logger.info(f"Transforming finished with success, created DataFrame shape: {df.shape[0]}, {df.shape[1]}")        
        return df       
             
    def merge_all_data(self, weather_data_raw, pm25_data_raw):
        '''
        Merges dataframes
        '''      
        weather_data_df = self.weather_data(weather_data_raw)
        pm25_data_df = self.pm25_data(pm25_data_raw)
        
        logger.info("Merging weather data with PM2.5 data")
        df = pd.merge(weather_data_df, pm25_data_df, on="city", how="left")
            
        logger.info(f"Merge finished with success, created DataFrame shape: {df.shape[0]}, {df.shape[1]}")
        return df
    
    def clean_data(self, df):
        '''
        Standardise column types and adds timestamp
        ''' 
        logger.info("Standardising merged dataframe")   
            
        timestamp_utc = pd.Timestamp.now(tz="UTC").floor("s")
        
        int_cols = ["pressure", "humidity" ,"sea_level" ,"grnd_level", "wind_deg", "pm25_sensor_id"]
        obj_cols = ["city"]
        float_cols = ["pm25_last_value[ug/m3]", "temp" ,"feels_like" ,"temp_min" ,"temp_max","wind_speed"]
        time_cols = ["weather_last_measurement_utc","pm25_last_measurement_utc"]
        
        try:
            
            df["downloaded_utc"] = pd.to_datetime(timestamp_utc)
            df["downloaded_utc"] = df["downloaded_utc"].astype("datetime64[s, UTC]")
        
            for col in df.columns:
                if col in int_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int32")
                    
                elif col in obj_cols:
                    df[col] = df[col].astype("object")   
                    
                elif col in float_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                elif col in time_cols:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.floor('s').astype('datetime64[s, UTC]')
            logger.info("Standardising finished with success")
           
        except Exception as e:
            logger.error(f"Failed to standardise data: {e}")
        
        try:
            logger.info("Categorising air quality. Classification based on US EPA (Environmental Protection Agency) NowCast methodology and epidemiologic studies on PM2.5 exposure-response relationship.")    
            df["air_quality_category"] = "Unknown"

            conditions = [
                df["pm25_last_value[ug/m3]"].isna(),
                df["pm25_last_value[ug/m3]"] <= 12,
                (df["pm25_last_value[ug/m3]"] > 12) & (df["pm25_last_value[ug/m3]"] <= 35.4),
                (df["pm25_last_value[ug/m3]"] > 35.4) & (df["pm25_last_value[ug/m3]"] <= 55.4),
                df["pm25_last_value[ug/m3]"] > 55.4
            ]

            choices = ["Unknown", "Good", "Moderate", "Unhealthy for sensitive groups", "Unhealthy"]

            df["air_quality_category"] = np.select(conditions, choices, default="Unknown")
               
        except Exception as e:
            logger.error(f"Failed to categorise PM2.5: {e}")
        
        logger.info("Category assigned")    
        return df, timestamp_utc