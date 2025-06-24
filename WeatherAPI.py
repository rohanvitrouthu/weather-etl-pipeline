import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Defining API endpoint for a specific location (Seattle, WA in this case)
api_url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 47.67,
    "longitude": -122.11,
    "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m"
}

# Sending request to the API
response = requests.get(api_url, params = params)

# Checking if the request was successful
if response.status_code == 200:
    data = response.json()
    print("Data extracted successfully")

    # The hourly key contains the main dataset
    hourly_data = data.get('hourly', {})
else:
    print(f"Failed to retrieve the data. Status code: {response.status_code}")
    hourly_data = {}

# The data for each attribute like temperature, relative humidity and wind speed are in parallel lists.
print(hourly_data)



# Loading the JSON data into a Pandas dataframe
if hourly_data:
    hourly_weather_df = pd.DataFrame(hourly_data)

    # 1. Data type conversion
    hourly_weather_df['time'] = pd.to_datetime(hourly_weather_df['time'])
    numeric_cols = ['temperature_2m', 'relative_humidity_2m', 'wind_speed_10m']
    for col in numeric_cols:
        hourly_weather_df[col] = pd.to_numeric(hourly_weather_df[col], errors='coerce')
        # 'coerce' turns errors into NaT (Not a Time)/NaN (Not a Number)

    # 2. Handling missing values
    # Checking for missing values
    print("Missing values per column:\n", hourly_weather_df.isnull().sum())

    #Drop rows with any missing values
    hourly_weather_df.dropna(inplace=True)

    # 3. Rename columns in hourly_weather_df dataframe for clarity
    hourly_weather_df.rename(columns = {
        "time": "forecast_timestamp",
        "temperature_2m": "temperature_celsius",
        "relative_humidity_2m": "relative_humidity_percent",
        "wind_speed_10m": "wind_speed_kmh" 
    }, inplace = True)

    # 4. Add metadata from the API call i.e. Latitude and Longitude of the location for which we are querying the weather.
    hourly_weather_df['latitude'] = params['latitude']
    hourly_weather_df['longitude'] = params['longitude']

    print("Data cleaned and necessary transformations performed successfully.")
    print(hourly_weather_df.tail(5))

else:
    print("No data to process.")

# Connecting to PostgreSQL database "weatherdb" on Port 5432 using psycopg2
"""conn = psycopg2.connect(
    dbname = "weatherdb",
    user = "rohanv52",
    password = "Rv7@Hyd/Ind",
    host = "localhost",
    port = "5432"
)"""

# Connecting to PostgreSQL database "weatherdb" on Port 5432 using sqlalchemy
# Ensuring the dataframe "hourly_weather_df" from the previous step exists
if "hourly_weather_df" in locals() and not hourly_weather_df.empty:
    # 1. Create a database connection engine
    db_user = ""
    db_pwd = ""
    db_host = "localhost"
    db_port = "5432"
    db_name = "weatherdb"

    encoded_pwd = quote_plus(db_pwd) # To handle special characters like '@', '/', etc.
    engine = create_engine(f'postgresql://{db_user}:{encoded_pwd}@{db_host}:{db_port}/{db_name}')

    # 2. Load the dataframe into PostgreSQL
    table_name = "hourly_weather_forecasts"
    try:
        hourly_weather_df.to_sql(table_name,
                                 engine,
                                 if_exists = "append",
                                 index = False) # 'Append' adds data. 'Replace' drops and recreates the table.
        print(f"Data successfully loaded into {table_name} table.")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    
else:
        print("Dataframe is empty or does not exist. Skipping database load.")