from datetime import datetime
import os
import pandas as pd
import requests
from typing import Tuple

AUTH_KEY = '' # Add in your api_key here

def api_call(api_key: str, city: str, state: str, country: str = 'us') -> dict:
    '''
    Makes request to openweather map with all parameters 
        and returns formatted dict with all necessary information

    Params:
    api_key: string of api_key provided by openweathermap api
    city: city you want to query
    state: state in which city resides
    country: country in which city resides, defaults to US unless otherwise specified

    For example, returns:
    {   
        'city, state': 'austin, tx',
        'dt': '2021-09-19 00:01:07',
        'feels_like': 95.14,
        'humidity': 43,
        'pressure': 1011,
        'temp': 92.05,
        'temp_max': 95.27,
        'temp_min': 88.29
    }
    '''
    weather_url = 'https://api.openweathermap.org/data/2.5/weather?q={CITY},{STATE},{COUNTRY}&appid={API_KEY}&units=imperial&dt' \
                    .format(CITY=city, STATE=state, COUNTRY=country, API_KEY=api_key)
    response = requests.get(weather_url).json()
    weather = response['main']
    weather['city, state'] = city + ", " + state
    weather['dt'] = datetime.utcfromtimestamp(response['dt']).strftime('%Y-%m-%d')

    weather = [weather]
    return weather

def _init_df(csv_path: str) -> pd.DataFrame:
    '''
    Takes in csv (if it exists) and returns Dataframe.
        If it doesn't exist, it initializes an empty dataframe

    Params:
    csv_path: csv file path for all collected weather data so far
    '''
    try: 
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
    except:
        df = pd.DataFrame()

def _column_renamer(df: pd.DataFrame, weather_list: list) -> Tuple[pd.DataFrame, str]:
    '''
    Takes in dataframe and returns a renamed dataframe 
        according to the city the data is for

    Params:
    df: input dataframe
    weather_list: list of dict containing weather data and city name
    '''
    city_name = weather_list[0]['city, state'].split(',')[0]
    df.columns = city_name + ' ' + df.columns
    return df, city_name

def _column_creator(city_name1: str, city_name2: str, attribute: str) -> Tuple[str, str]:
    '''
    Helper function to create the names needed to do diff calculations in update_df

    Params:
    city_name: city name passed in
    attribute: attribute you wish to track from original weather data
    '''
    return city_name1 + ' ' + attribute, city_name2 + ' ' + attribute

def update_df(weather1: list, weather2: list, csv_path: str = None) -> None:
    '''
    Reads in csv_path to a dataframe and updates that dataframe with today's combined weather for comparison

    Params:
    weather1: list[dict] of weather for one city
    weather2: list[dict] of weather for the other city
    csv_path: if adding to an existing csv, defaults to none
    '''

    old_df = _init_df(csv_path)

    weather_one_df = pd.DataFrame(weather1)
    weather_two_df = pd.DataFrame(weather2)

    weather_one_df, city_name_one = _column_renamer(weather_one_df, weather1)
    weather_two_df, city_name_two = _column_renamer(weather_two_df, weather2)

    combined_df = pd.concat([weather_one_df, weather_two_df], axis=1)
    combined_df = combined_df[[item for items in zip(weather_one_df.columns, weather_two_df.columns) for item in items]]

    # Column names programmatically created
    city_one_temp, city_two_temp = _column_creator(city_name_one, city_name_two, 'temp')
    city_one_humidity, city_two_humidity = _column_creator(city_name_one, city_name_two, 'humidity')
    city_one_pressure, city_two_pressure = _column_creator(city_name_one, city_name_two, 'pressure')
    city_one_temp_max, city_two_temp_max = _column_creator(city_name_one, city_name_two, 'temp_max')
    city_one_temp_min, city_two_temp_min = _column_creator(city_name_one, city_name_two, 'temp_min')
    city_one_dt, city_two_dt = _column_creator(city_name_one, city_name_two, 'dt')

    combined_df['temp_diff'] = combined_df[city_one_temp] - combined_df[city_two_temp]
    combined_df['humidity_diff'] = combined_df[city_one_humidity] - combined_df[city_two_humidity]
    combined_df['pressure_diff'] = combined_df[city_one_pressure] - combined_df[city_two_pressure]
    combined_df['temp_max_diff'] = combined_df[city_one_temp_max] - combined_df[city_two_temp_max]
    combined_df['temp_min_diff'] = combined_df[city_one_temp_min] - combined_df[city_two_temp_min]

    new_df = pd.concat([old_df, combined_df], axis=0)
    new_df = new_df.drop_duplicates(subset=[city_one_dt, city_two_dt])

    if csv_path:
        new_df.to_csv(csv_path, index=False)
    else:
        new_df.to_csv('{city1}_and_{city2}_weather_comparison.csv'.format(city1=city_name_one, city2=city_name_two), index=False)


def main() -> bool:
    '''
    Changing params within this function will lead to different results based on city and state chose
        Returns true based on successful update and false on error
    '''
    try:
        weather_one = api_call(AUTH_KEY, 'austin', 'tx')
        weather_two = api_call(AUTH_KEY, 'saratoga', 'ca')

        update_df(weather_one, weather_two)

        return True
    except:
        return False

if __name__ == '__main__':
    main()