'''
Module for downloading data
'''
import requests
import logging
import json
import os
import pandas as pd
import datetime as dt
import time

from zoneinfo import ZoneInfo
from openaq import OpenAQ
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Downloader:
    '''
    Downloads data from openweathermap.org and openaq.org
    '''
    
    def __init__(self, config):
        '''
        Initialize downloader with configuration
        '''
                   
        self.config = config
        self.units = config["display"]["units"]
        self.radius = config["air_quality_stations"]["radius"]
       
    def validate_location(self, location_config):
        '''
        Checks validity of provided location
        '''
        
        required = ["latitude", "longitude"]
        for key in required:
            if key not in location_config:
                raise ValueError(f"Missing {key} in location config")
            if not isinstance(location_config[key], (int, float)):
                raise ValueError(f"{key} must be a number")
        
        latitude = location_config["latitude"]
        longitude = location_config["longitude"]
        
        if (-90 <= latitude) & (latitude <= 90):
            logger.info("Latitude check passed")
        else:
            raise ValueError(f"{latitude} is not valid value, must be in [-90; 90]")
        if (-180 <= longitude) & (longitude <= 180):
            logger.info("Longitude check passed")
        else:
            raise ValueError(f"{longitude} is not valid value, must be in [-180; 180]")
      
    def get_current_weather_data_location(self, location_config, api_key):
        '''
        Get data from Current Weather Data API for given location
        '''      
        # collect location from passed config
        latitude = location_config["latitude"]
        longitude = location_config["longitude"]
        units = self.units
        
        # parse url
        url = self.config["url"]["openweathermap"]
        url = url + f"lat={latitude}&lon={longitude}&appid={api_key}&units={units}"
        
        # API call for location
        for attempt in range(3):        
            try:
                request = requests.get(url, timeout=10)
                request.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit, waiting 30s")
                    time.sleep(30)
                else:
                    logger.error(f"HTTP error: {e}")
                    time.sleep(5)
            except Exception as e:
                logger.error(f"API error: {e}")
                time.sleep(5)
        else:
            raise RuntimeError("Open Weather Map API failed after retries")
            
        try:
            response = json.loads(request.content)
            response_main = pd.DataFrame([response["main"]])
            response_wind = pd.DataFrame([response["wind"]])[["speed", "deg"]].rename(columns={"speed":"wind_speed", "deg":"wind_deg"})
            response_dt = pd.DataFrame([response["dt"]]).rename(columns={0:"unix_datetime"})
            weather = pd.concat([response_main,response_wind, response_dt], axis=1)
            return weather
        except Exception as e:
            logger.error(f"Failed to collect data: {e}")
            raise
    
    def get_pm25_sensor(self, location_config, air_quality_api_key):
        '''
        Get air quality sensor ID from OpenAQ for given location for PM2.5. Limited to last measurement from today and closest station to given location.
        '''  
        # collect location from passed config
        latitude = location_config["latitude"]
        longitude = location_config["longitude"]
        radius = self.radius        
        
        url = f"https://api.openaq.org/v3/locations?coordinates={latitude},{longitude}&radius={radius}&limit=1000"
        header = {"X-API-Key": air_quality_api_key}
        
        for attempt in range(3):
            try:
                request = requests.get(url, headers=header, timeout=10)
                request.raise_for_status()
                break
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit, waiting 30s")
                    time.sleep(30)
                else:
                    logger.error(f"HTTP error: {e}")
                    time.sleep(5)
            except Exception as e:
                logger.error(f"API error: {e}")
                time.sleep(5)
        else:
            raise RuntimeError("OpenAQ API failed after retries")

        try:
            results = json.loads(request.content)
            results = pd.json_normalize(results["results"])
                
            if results.empty:
                logger.error(f"No PM2.5 active sensor found in {radius}[m] radius")
                sensor_id = None
                return sensor_id
                
            if results.empty:
                logger.error(f"No PM2.5 active sensor found in {radius}[m] radius")
                sensor_id = None
                return sensor_id
                
            results = results.sort_values(by="distance", ascending=True).head(5)
            results = results[["sensors"]]
            results_expanded = results.explode("sensors")
            results_expanded = results_expanded["sensors"].apply(pd.Series).rename(columns={"id":"sensor_id"})
            results_expanded = pd.concat([results_expanded.drop(columns=["parameter"]), results_expanded["parameter"].apply(pd.Series)], axis=1)
            sensor_id = results_expanded[results_expanded["id"] == 2]
            sensor_id = list(sensor_id["sensor_id"].astype("Int64"))
            return sensor_id
        
        except Exception as e:
            logger.error(f"Failed to collect data: {e}")
            raise 
    
    
    def get_current_pm25_data_location(self, sensors_list, air_quality_api_key):
        header = {"X-API-Key": air_quality_api_key}
        
        # try next sensor if this one failed
        for sensor_id in sensors_list:
            url = f"https://api.openaq.org/v3/sensors/{sensor_id}"
            
            for attempt in range(3):
                try:
                    request = requests.get(url, headers=header, timeout=10)
                    request.raise_for_status()
                    break
                except Exception as e:
                    logger.warning(f"Sensor {sensor_id} failed: {e}")
                    time.sleep(2)
            else:
                continue

            try:
                results = json.loads(request.content)
                results = pd.json_normalize(results["results"])
                results = results[["id", "datetimeLast.utc", "latest.value"]]
                
                results["datetimeLast.utc"] = pd.to_datetime(results["datetimeLast.utc"])
                cutoff = dt.datetime.now(tz=ZoneInfo("UTC")) - dt.timedelta(hours=24)
                
                results = results[results["datetimeLast.utc"] > cutoff]
                
                # Check if DataFrame is empty after cutoff filter
                if results.empty:
                    logger.warning(f"Sensor {sensor_id} has no recent data (>24h old), trying next")
                    continue
                
                results = results.rename(columns={"id":"sensor_id"})
                return results
                
            except (KeyError, IndexError) as e:
                logger.warning(f"Sensor {sensor_id} has no data: {e}, trying next")
                continue

        # No working sensor found - return empty DataFrame
        logger.error("No working PM2.5 sensor found, returning empty data")
        return pd.DataFrame({
            "sensor_id": [pd.NA],
            "datetimeLast.utc": [pd.NaT],
            "latest.value": [pd.NA]
        })
    
    
    def get_all_locations(self):
        '''
        Download current weather and air quality for all provided locations
        '''        
        # collect secrets
        load_dotenv()
        weather_api_key = os.getenv("OPENWEATHER_API_KEY")
        air_quality_api_key = os.getenv("OPENAQ_API_KEY")
        
        weather_data = []
        pm25_data = []
  
        # locations dict
        locations = self.config.get("location",{})
        
        if not locations:
            raise ValueError("No data provided")
        
        for location, location_config in locations.items():
            logger.info(f"Collecing data for {location}")
            
            self.validate_location(location_config)
            
            weather_location_data = self.get_current_weather_data_location(location_config, weather_api_key)
            logger.info(f"Weather data for {location} found")
            
            sensors_list = self.get_pm25_sensor(location_config, air_quality_api_key)
            logger.info(f"Active sensors with measurements in last 24h found")
                       
            if sensors_list:
                pm25_location_data = self.get_current_pm25_data_location(sensors_list, air_quality_api_key)
                pm25_data.append(pm25_location_data)
                logger.info(f"Last measurement from the nearest active sensor added")
            else:
                logger.warning("No PM2.5 sensor found")
                pm25_location_data = pd.DataFrame({
                        "datetimeLast.utc": [pd.NaT],
                        "latest.value": [pd.NA]
                    })
            
            weather_location_data["city"] = location
            pm25_location_data["city"] = location
            
            weather_data.append(weather_location_data)
            
        weather_data = pd.concat(weather_data, ignore_index=True)
        pm25_data = pd.concat(pm25_data, ignore_index=True)
           
        if len(weather_data) == 1:
            logger.info(f"Weather data for {len(weather_data)} city collected")
        elif len(weather_data) > 1:
            logger.info(f"Weather data for {len(weather_data)} cities collected")
            
        if len(pm25_data) == 1:
            logger.info(f"PM2.5 data for {len(pm25_data)} city collected")
        elif len(pm25_data) > 1:
            logger.info(f"PM2.5 data for {len(pm25_data)} cities collected")
        
        logger.info(f"Successfully collected data for {len(weather_data)} locations")
        return weather_data, pm25_data