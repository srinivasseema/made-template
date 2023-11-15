import pandas as pd
from sqlalchemy import create_engine

class AirportData:
    @staticmethod
    def executePipeline():
        # Create a SQLAlchemy engine to connect to the SQLite database.
        engine = create_engine('sqlite:///airports.sqlite')

        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv('https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv',header=0, sep=";")

        # Mapping of data type to each column
        df = df.astype({
            'column_1': 'int64',
            'column_2': 'str',
            'column_3': 'str',
            'column_4': 'str',
            'column_5': 'str',
            'column_6': 'str',
            'column_7': 'float64',
            'column_8': 'float64',
            'column_9': 'int64',
            'column_10': 'float32',
            'column_11': 'str',
            'column_12': 'str',
            'geo_punkt': 'str'
        })

        #Writing csv data into table airports.
        df.to_sql('airports', engine, if_exists='replace',index=False)

        #Sample data read
        print('Sample data read from airports')
        df_read = pd.read_sql('SELECT * FROM airports', engine)
        print(df_read.head().to_string)

        engine.dispose()


if __name__ == "__main__":
    AirportData.executePipeline()
