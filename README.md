# Weather & Air Quality Dashboard

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An automated ETL pipeline that collects real-time weather and air quality data from public APIs, processes it, and stores it in a SQLite database for analysis. The project demonstrates data engineering skills including API integration, error handling, data transformation, and storage.

## Research Questions

* **Which Polish cities currently have the worst air quality?**
* **Is there a correlation between wind speed and PM2.5 levels?**
* **What percentage of cities meet US EPA air quality standards (PM2.5 ≤ 12 μg/m³)?**
* **What is the data availability/quality for PM2.5 sensors across Polish cities?**

## Key Insights

* **Air Quality Distribution**: 62 Polish cities were analyzed, with **only 5% meeting US EPA "Good" air quality standards** (PM2.5 ≤ 12 μg/m³).
* **Critical Pollution**: **Łask recorded PM2.5 level of 40.6 μg/m³** (Unhealthy for Sensitive Groups category), exceeding US EPA moderate threshold.
* **Data Coverage**: PM2.5 sensor data is available for approximately **20% of analyzed cities**, with gaps primarily in smaller municipalities due to limited sensor infrastructure.
* **Wind speed Independence**: Weak correlation (r ≈ 0.13) between wind speed and PM2.5 levels suggests pollution is driven more by industrial activity and traffic than weather patterns.

## Technical Stack

* **Python 3.12+**
* **pandas** – Data manipulation and transformation
* **numpy** – Numerical operations and data categorization
* **requests** – API communication with retry logic
* **sqlite3** – Data persistence and storage
* **matplotlib / seaborn** – Data visualization
* **jupyter** – Analysis environment
* **python-dotenv** – Secure API key management
* **PyYAML** – Configuration management

## Project Structure

```bash
weather_&_air_quality_dashboard/
├── data/
│   └── weather.db              # SQLite database (generated)
├── src/
│   ├── __init__.py
│   ├── downloader.py           # API data extraction
│   ├── transformer.py          # Data cleaning and categorization
│   └── loader.py               # Database storage
├── analysis_notebook.ipynb     # Data analysis and visualization
├── main.py                     # ETL pipeline orchestration
├── config.yaml                 # API endpoints and city coordinates
├── .env                        # API keys (not tracked in git)
├── requirements.txt
├── README.md
└── LICENSE
```

## ETL Pipeline Architecture

### 1. **Extract** (downloader.py)

* Fetches current weather data from OpenWeatherMap API for 313 Polish cities
* Retrieves PM2.5 air quality measurements from OpenAQ API
* Implements retry logic with exponential backoff for API failures
* Validates geographic coordinates and handles rate limiting
* Finds nearest active PM2.5 sensors within 12km radius of each city

### 2. **Transform** (transformer.py)

* Merges weather and air quality data by city
* Standardizes data types (integers, floats, timestamps)
* Categorizes PM2.5 levels using US EPA NowCast methodology:
  * **Good**: ≤ 12 μg/m³
  * **Moderate**: 12.1–35.4 μg/m³
  * **Unhealthy for Sensitive Groups**: 35.5–55.4 μg/m³
  * **Unhealthy**: > 55.4 μg/m³
* Handles missing sensor data and measurement gaps

### 3. **Load** (loader.py)

* Stores processed data in SQLite database with timestamp-based deduplication
* Prevents duplicate entries within the same hour
* Creates database schema automatically on first run
* Supports incremental data loading for time-series analysis

## Usage

### 1. Clone the repository

```bash
git clone https://github.com/tomszy91/weather_&_air_quality_dashboard
cd weather_air_quality_dashboard
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up API keys

Create a `.env` file in the project root:

```env
OPENWEATHER_API_KEY=your_openweathermap_api_key
OPENAQ_API_KEY=your_openaq_api_key
```

Get free API keys:

* [OpenWeatherMap](https://openweathermap.org/api)
* [OpenAQ](https://openaq.org/)

### 4. (Optional) Customize city list

Edit `config.yaml` to add/remove cities:

```yaml
location:
  YourCity:
    latitude: 52.23
    longitude: 21.02
```

### 5. Run the ETL pipeline

```bash
python main.py
```

This will:

* Download current weather and air quality data
* Transform and categorize the data
* Save results to `data/weather.db`

### 6. Analyze the data

```bash
jupyter notebook analysis_notebook.ipynb
```

The analysis notebook includes:

* Air quality rankings by city
* Wind speed vs PM2.5 correlation analysis
* Data quality assessment
* Time-series trends (if multiple runs)
* Geographic distribution of pollution levels

## Data Sources

* **Weather Data**: [OpenWeatherMap Current Weather API](https://openweathermap.org/current)
* **Air Quality Data**: [OpenAQ API v3](https://docs.openaq.org/)
* **PM2.5 Categorization**: [US EPA Air Quality Index](https://www.airnow.gov/aqi/aqi-basics/)

## Executive Summary

This project demonstrates a production-ready ETL pipeline that integrates multiple external APIs, handles real-world data quality issues (missing sensors, API timeouts, measurement gaps), and provides actionable air quality insights for Polish cities.

The analysis reveals variation in air quality, only 26% of analyzed cities meet WHO health standards for PM2.5 exposure, highlighting the ongoing air quality challenges in Poland.

The pipeline is designed for automation and can be scheduled (via cron/Task Scheduler) to collect hourly measurements, enabling trend analysis and pollution forecasting. Key technical achievements include robust error handling, data validation, and efficient incremental loading to SQLite.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
