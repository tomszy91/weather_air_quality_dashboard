from src.downloader import Downloader
from src.transformer import Transformer
from src.loader import Loader

import yaml
import datetime as dt
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_path="config.yaml"):
    '''
    Docstring for load_config
    Load configuration from YAML file
    '''
    with open(config_path, "r", encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def main():
    '''
    Main execution flow
    '''
    logger.info("Starting processing Weather & Air Quality Dashboard")
    config = load_config()
    logger.info("Configuration loaded")
    
    downloader = Downloader(config)
    weather_data, pm25_data = downloader.get_all_locations()

    if weather_data.empty or pm25_data.empty:
        raise ValueError("Data not completed")
       
    transformer = Transformer()
    merged_df = transformer.merge_all_data(weather_data, pm25_data)
    df, timestamp = transformer.clean_data(merged_df)
    
    loader = Loader(config)
    loader.save_dataframe(df, timestamp)

if __name__ == "__main__":
    main()