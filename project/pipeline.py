import kaggle
import os
import pandas as pd 
import logging
from sqlalchemy import create_engine


#About Dataset

#This dataset is a daily time series of electricity demand, generation and prices in Spain from 2014 to 2018. It is gathered from ESIOS, a website managed by REE (Red Electrica Española) which is the Spanish TSO (Transmission System Operator)

#A TSO main function is to operate the electrical system and to invest in new transmission (high voltage) infrastructure.
#(https://www.ree.es/en/about-us/business-activities/electricity-business-in-Spain)
#As a system operator, REE forecast electricity demand and offer and runs daily actions . As a result of daily actions, a PBF Plan Básico de Funcionamiento) is yielded. This is a basic schedulling of energy production (upon it several mechanisms are triggerd to ensure supply)
#Enery and prices data can be downloaded from : https://www.esios.ree.es/en
#This data is publicly available via ENTSOE and REE and may be found in the links above.

#Weather data is from the Open Weather API
#https://openweathermap.org/api


class EnergyDataPipeline:

    # Define a class-level logger object
    logger = logging.getLogger(__name__)
    # Initialize energyDF and weatherDF as class attributes
    energyDF = None
    weatherDF = None
    engine = None


    def __init__(self):
        # Configure the logger
        self.logger.setLevel(logging.INFO)
        # Create a SQLAlchemy engine to connect to the SQLite database.
        self.engine = create_engine('sqlite:///energystorm.sqlite')
    
    def downloadfiles(self):
        # Authenticate with the Kaggle API
        self.logger.info('Authenticating with Kaggle API')
        kaggle.api.authenticate()

        # Specify the dataset to download
        dataset_name = "nicholasjhana/energy-consumption-generation-prices-and-weather"

        # Create a data folder if it doesn't exist
        data_folder = "data"
        
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        # Download the dataset
        self.logger.info("Downloading dataset %s", dataset_name)
        kaggle.api.dataset_download_files(dataset_name, path=data_folder, unzip=True)

 
    
    def readfiles(self):

        # Read energy data
        energyDataPath = "./data/energy_dataset.csv"
        self.logger.info("Reading dataset %s", energyDataPath)
        self.energyDF = pd.read_csv(f"{energyDataPath}")

        # Preview of Energy data
        self.energyDF.head(5)

        # Read Weather data
        weatherDataPath = "./data/weather_features.csv"
        self.logger.info("Reading dataset ", weatherDataPath)
        self.weatherDF = pd.read_csv(f"{weatherDataPath}")

        # Preview of Weather data
        self.weatherDF.head(5)

    def data_exploration(self):
        self.energyDF.info()
        self.energyDF.describe() 
        self.weatherDF.info()
        self.weatherDF.describe()


    def executePipeline(self):
        #make pipeline sections
        self.downloadfiles()
        self.readfiles()
        self.data_exploration()
        self.writeDataToSQL()

    def writeDataToSQL(self):
        self.energyDF.to_sql('energy', self.engine, if_exists='replace',index=False)
        self.weatherDF.to_sql('weather', self.engine, if_exists='replace',index=False)

    
if __name__ == "__main__":
     pipeline = EnergyDataPipeline()
     pipeline.executePipeline()