import kaggle
import os


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
    
    @staticmethod
    def executePipeline():
        
        # Authenticate with the Kaggle API
        kaggle.api.authenticate()

        # Specify the dataset to download
        dataset_name = "nicholasjhana/energy-consumption-generation-prices-and-weather"

        # Create a data folder if it doesn't exist
        data_folder = "data"
        
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        # Download the dataset
        kaggle.api.dataset_download_files(dataset_name, path=data_folder, unzip=True)

if __name__ == "__main__":
    EnergyDataPipeline.executePipeline()