import urllib.request
import os
import pandas as pd
import sqlite3
from zipfile import ZipFile

def process_gtfs_data():
    """
    Downloads GTFS data from a specified URL, extracts the 'stops.txt' file,
    filters and validates the data, and writes it into an SQLite database.

    Returns:
        None
    """
    # Constants
    URL = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
    ZIP_PATH = 'data/GTFS.zip'
    DB_PATH = 'gtfs.sqlite'
    REQUIRED_COLUMNS = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']

    # Download the ZIP file
    urllib.request.urlretrieve(URL, ZIP_PATH)

    # Extract and read the 'stops.txt' file
    with ZipFile(ZIP_PATH, 'r') as zip_file:
        with zip_file.open('stops.txt') as stops_file:
            # Load the file into a pandas DataFrame
            stops_df = pd.read_csv(stops_file)

    # Filter and validate the data
    valid_stops = filter_and_validate_stops(stops_df, REQUIRED_COLUMNS)
    # Write the data into the SQLite database
    write_to_database(valid_stops, DB_PATH)

    # Clean up resources
    clean_up(ZIP_PATH)

def filter_and_validate_stops(stops_df, required_columns):
    """
    Filters and validates the stops DataFrame.

    Args:
        stops_df (pd.DataFrame): The stops DataFrame.
        required_columns (list): List of required columns.

    Returns:
        pd.DataFrame: The filtered and validated stops DataFrame.
    """
    # Filter data for zone 2001 and select required columns
    filtered_stops = stops_df[stops_df['zone_id'] == 2001][required_columns]

    # Validate latitude and longitude
    valid_stops = filtered_stops[(filtered_stops['stop_lat'].between(-90, 90)) & 
                                 (filtered_stops['stop_lon'].between(-180, 180))]

    # Remove rows with missing or invalid data
    valid_stops.dropna(inplace=True)

    return valid_stops

def write_to_database(valid_stops, db_path):
    """
    Writes the valid stops DataFrame into an SQLite database.

    Args:
        valid_stops (pd.DataFrame): The valid stops DataFrame.
        db_path (str): The path to the SQLite database.

    Returns:
        None
    """
    # Connect to the SQLite database
    with sqlite3.connect(db_path) as conn:
        # Write the data into the database
        valid_stops.to_sql('stops', conn, if_exists='replace', index=False, dtype={
            'stop_id': 'INTEGER',
            'stop_name': 'TEXT',
            'stop_lat': 'REAL',
            'stop_lon': 'REAL',
            'zone_id': 'INTEGER'
        })

def clean_up(zip_path):
    """
    Cleans up resources by deleting the ZIP file.

    Args:
        zip_path (str): The path to the ZIP file.

    Returns:
        None
    """
    # Delete the ZIP file
    os.remove(zip_path)

if __name__ == '__main__':
    process_gtfs_data() # Call the main function
    