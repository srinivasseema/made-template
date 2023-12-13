import kaggle
import os
import pandas as pd 
import logging
from sqlalchemy import create_engine
from prefect import flow, task

class EnergyDataPipeline:
    
    @task
    def downloadfiles():
        logger = logging.getLogger(__name__)
        # Authenticate with the Kaggle API
        logger.info('Authenticating with Kaggle API')
        kaggle.api.authenticate()

        # Specify the dataset to download
        dataset_name = "nicholasjhana/energy-consumption-generation-prices-and-weather"

        # Create a data folder if it doesn't exist
        data_folder = "data"
        
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        # Download the dataset
        logger.info("Downloading dataset %s", dataset_name)
        kaggle.api.dataset_download_files(dataset_name, path=data_folder, unzip=True)

    @task   
    def readEnergyData():
        logger = logging.getLogger(__name__)
        # Read energy data
        energyDataPath = "./data/energy_dataset.csv"
        logger.info("Reading dataset %s", energyDataPath)
        energyDF = pd.read_csv(f"{energyDataPath}")

        # Preview of Energy data
        print(energyDF.head(5))
        return energyDF

    @task   
    def readWeatherData():
        logger = logging.getLogger(__name__)
       
        # Read Weather data
        weatherDataPath = "./data/weather_features.csv"
        logger.info("Reading dataset ", weatherDataPath)
        weatherDF = pd.read_csv(f"{weatherDataPath}")

        # Preview of Weather data
        print(weatherDF.head(5))
        return weatherDF
    @task   
    def readfiles():
        logger = logging.getLogger(__name__)
        # Read energy data
        energyDataPath = "./data/energy_dataset.csv"
        logger.info("Reading dataset %s", energyDataPath)
        energyDF = pd.read_csv(f"{energyDataPath}")

        # Preview of Energy data
        energyDF.head(5)

        # Read Weather data
        weatherDataPath = "./data/weather_features.csv"
        logger.info("Reading dataset ", weatherDataPath)
        weatherDF = pd.read_csv(f"{weatherDataPath}")

        # Preview of Weather data
        weatherDF.head(5)

    @task
    def plotHeatmap(df):
        import seaborn as sns
        import matplotlib.pyplot as plt
        
        plt.figure(figsize=(12,10))
        sns.heatmap(df.corr(), annot=True, cmap=plt.cm.Reds)
        plt.show()

    @task
    def null_values(tableName, df):
        #Null % check amongst columns
        dfWithNull = round((df.isnull().sum()/len(df)*100),2)
        print("null values of "+tableName)
        print(dfWithNull.sort_values(ascending=False))
        return dfWithNull

    @task
    def findCorrelations(df):
        # DF.corr to show correlation of values
        # Assuming df is your DataFrame and 'date' is the column with the date strings
        # Convert 'time' column to datetime
        df['time'] = pd.to_datetime(df['time'], utc=True)
        # Convert 'time' column to Unix timestamp
        df['time'] = df['time'].apply(lambda x: x.timestamp())
        correlations = df.corr(method='pearson')
        print(correlations['price actual'].sort_values(ascending=False).to_string())
        # Heatmap of correlation
        zero_val_cols = ['generation marine',
                 'generation geothermal',
                 'generation fossil peat',
                 'generation wind offshore',
                 'generation fossil oil shale',
                 'forecast wind offshore eday ahead',
                 'generation fossil coal-derived gas',
                 'generation hydro pumped storage aggregated']
        correlations = correlations.drop(columns=zero_val_cols,axis=1)
        return correlations

    @task
    def data_exploration(df):
        df.info()
        df.describe()
        return df

    @task
    def writeDataToSQL(tableName, df):
        logger = logging.getLogger(__name__)
        logger.info("Writing dataset %s to SQL", tableName)
        engine = create_engine('sqlite:///energystorm.sqlite')
        df.to_sql(tableName, engine, if_exists='replace',index=False)