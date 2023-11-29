import logging
import sqlite3
import pandas as pd

# Configure logging
#logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
# Create a logger object
logger = logging.getLogger(__name__)

# Set the logging level
logger.setLevel(logging.INFO)
handler = logging.FileHandler('my_logfile.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def cleanup(data, column):
    if column in data.columns:
        data.drop(column, axis=1, inplace=True)
    else:
        logging.warning(f'Column {column} does not exist in the DataFrame')
    
    return data

def validate_data(data):
    valid_verkehr = ['FV', 'RV', 'nur DPN']
    valid_ifopt_pattern = r'^[A-Za-z]{2}:\d+:\d+(:\d+)?$'

    # Validate Verkehr
    invalid_verkehr = data[~data['Verkehr'].isin(valid_verkehr)]
    if not invalid_verkehr.empty:
        logging.warning(f'Invalid Verkehr values found: {invalid_verkehr.to_string()}')
        data.drop(invalid_verkehr.index, inplace=True)

    # Validate Laenge and Breite

    # Replace commas with periods and convert to float
    data['Laenge'] = data['Laenge'].str.replace(',', '.').astype(float)
    data['Breite'] = data['Breite'].str.replace(',', '.').astype(float)

    # perform the comparison
    invalid_coordinates = data[(data['Laenge'] < -90) | (data['Laenge'] > 90) | (data['Breite'] < -90) | (data['Breite'] > 90)]

    if not invalid_coordinates.empty:
        logging.warning(f'Invalid coordinates found: {invalid_coordinates.to_string()}')
        data.drop(invalid_coordinates.index, inplace=True)

    # Validate IFOPT
    # Check for missing values in 'IFOPT' column
    if data['IFOPT'].isnull().any():
        # Drop rows with missing 'IFOPT' values
        data.dropna(subset=['IFOPT'], inplace=True)
    

    # Now apply the match and ~ operators
    invalid_ifopt = data[~data['IFOPT'].str.match(valid_ifopt_pattern)]
    if not invalid_ifopt.empty:
        logging.warning(f'Invalid IFOPT values found: {invalid_ifopt.to_string()}')
        data.drop(invalid_ifopt.index, inplace=True)

    # Check for empty cells
    empty_cells = data.isnull().any(axis=1)
    if empty_cells.any():
        logging.warning(f'Empty cells found in rows: {data[empty_cells].index}')
        data.drop(data[empty_cells].index, inplace=True)
    
    print("processed output")
    print(data)

    return data

def create_database_connection():
    connection = sqlite3.connect('trainstops.sqlite')
    return connection

def create_trainstops_table(connection):
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trainstops (
            Rekordnummer BIGINT PRIMARY KEY,
            EVA_NR BIGINT,
            DS100 TEXT,
            IFOPT TEXT,
            NAME TEXT,
            Verkehr TEXT,
            Laenge FLOAT,
            Breite FLOAT,
            Betreiber_Name TEXT,
            Betreiber_Nr BIGINT)""")
    connection.commit()
    cursor.close()

def insert_data_into_table(connection, data):
    data.to_sql('trainstops', connection, if_exists='append', index=False)
    connection.commit()

def main():

    # Define the data types for each column
    data_types = {
        "EVA_NR": pd.Int64Dtype(),
        "DS100": pd.StringDtype(),
        "IFOPT": pd.StringDtype(),
        "NAME": pd.StringDtype(),
        "Verkehr": pd.StringDtype(),
        "Laenge": pd.StringDtype(),
        "Breite": pd.StringDtype(),
        "Betreiber_Name": pd.StringDtype(),
        "Betreiber_Nr": pd.Int64Dtype(),
        "Status": pd.StringDtype(),
    }

    # Read CSV data
    data = pd.read_csv('https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV', sep=";", dtype = data_types)

    print(data.head())
    # Cleanup Code
    columnToDrop = "Status"
    cleaned_data = cleanup(data.copy(), columnToDrop)

    print(cleaned_data.head())
   
    # Validate data
    validated_data = validate_data(cleaned_data)

    # Create database connection
    connection = create_database_connection()

    # Create trainstops table if not exists
    create_trainstops_table(connection)

    # Insert validated data into table
    insert_data_into_table(connection, validated_data)

    # Close database connection
    connection.close()

if __name__ == '__main__':
    main()
