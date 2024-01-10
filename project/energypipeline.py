import kaggle
import os
import pandas as pd 
import logging
import numpy as np
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
import seaborn as sns
import matplotlib.pyplot as plt

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
        

        # .corr heatmap of df to visualize correlation & show plot
        sns.heatmap(round(df.corr(),1),annot=True,cmap='Blues',linewidth=0.9)
        plt.show()

    @task
    def priceActualHisto(df):
        # Histogram of price actual
        plt.figure(figsize=(15,10))
        # Hist graph to show distribution of target variable
        sns.histplot(df,x='price actual')

    @task
    def wrangleData(df):
    # Read in the data, parse dates, and set the index
    # Assuming df is your DataFrame and 'time' is the column with dates
        #df['time'] = pd.to_datetime(df['time'])
        #df.set_index('time', inplace=True)

        df['time'] = pd.to_datetime(df['time'], utc=True)
        # Convert 'time' column to Unix timestamp
        df['time'] = df['time'].apply(lambda x: x.timestamp())
        df.set_index('time', inplace=True)
 
    # Rename columns by replacing all - or blank space with _
        df.columns = df.columns.str.replace(' ','_').str.replace('-','_')

        # Make the index DT
        df.index = pd.to_datetime(df.index, utc=True)    

    # Drop all columns with data leakage, or 90% + null
        df.drop(columns=['price_day_ahead',
                     'generation_marine',
                     'total_load_forecast',
                     'generation_geothermal',
                     'generation_fossil_peat',
                     'generation_wind_offshore',
                     'forecast_solar_day_ahead',
                     'generation_fossil_oil_shale',
                     'forecast_wind_onshore_day_ahead',
                     'forecast_wind_offshore_eday_ahead',
                     'generation_fossil_coal_derived_gas',
                     'generation_hydro_pumped_storage_aggregated'],inplace=True)
    
    # Drop Outlier row 2014 for plotting
        #df = df.drop(pd.Timestamp('2014-12-31 23:00:00+00:00')) 
    
    # Sort index
        df = df.sort_index()
    
    # Set conditional satements for filtering times of month to season value
        condition_winter = (df.index.month>=1)&(df.index.month<=3)
        condtion_spring = (df.index.month>=4)&(df.index.month<=6)
        condition_summer = (df.index.month>=7)&(df.index.month<=9)
        condition_automn = (df.index.month>=10)@(df.index.month<=12)
    
    # Create column in dataframe that inputs the season based on the conditions created above
        df['season'] = np.where(condition_winter,'winter',
                            np.where(condtion_spring,'spring',
                                     np.where(condition_summer,'summer',
                                              np.where(condition_automn,'automn',np.nan))))
        return df

    @task
    def pricePerTotalLoad(df):
        import plotly.express as px
        # Plot price actual vs total load
        # Figure showing Price per total load
        fig = px.scatter(df,x='total_load_actual',
                 y='price_actual',
                 facet_col='season',
                 opacity=0.1,
                 title='Price Per KW Hour Compaired To Total Energy Genereated Per Season',
                 animation_frame=df.index.year)

        # Figure customizations
        fig.update_traces(marker=dict(size=12,
                              line=dict(width=2,
                                        color='darkslateblue')),
                  selector=dict(mode='markers'))
        

    @task
    def splitDataAndBaseline(df):
        # Create Target variable
        from sklearn.model_selection import train_test_split, cross_val_score, validation_curve, GridSearchCV
        from sklearn.metrics import mean_absolute_error, mean_squared_error
        target='price_actual'

        # Split data into feature matrix and target vector
        y,X=df[target],df.drop(columns=target)

        # split data into train / validation sets
        X_train,X_val,y_train,y_val = train_test_split(X,y,test_size=.2,random_state=42)
        y_pred = [y_train.mean()]*len(y_train)
        mean_baseline_pred = y_train.mean()
        baseline_mae = mean_absolute_error(y_train,y_pred)
        baseline_rmse = mean_squared_error(y_train,y_pred,squared=False)

        # Print statement to show all baseline values
        print('Mean Price Per KW/h Baseline Pred:', mean_baseline_pred)
        print('-------------------------------------------------------------------')
        print('Baseline Mae:',baseline_mae)
        print('-------------------------------------------------------------------')
        print('Baseline RMSE:',baseline_rmse)

    @task
    def null_values(tableName, df):
        #Null % check amongst columns
        dfWithNull = round((df.isnull().sum()/len(df)*100),2)
        print("null values of "+tableName)
        print(dfWithNull.sort_values(ascending=False))
        return dfWithNull

    @task
    def findCorrelations(df):
        logger = get_run_logger()
        # DF.corr to show correlation of values
        # Assuming df is your DataFrame and 'date' is the column with the date strings
        # Convert 'time' column to datetime
        df['time'] = pd.to_datetime(df['time'], utc=True)
        # Convert 'time' column to Unix timestamp
        df['time'] = df['time'].apply(lambda x: x.timestamp())
        correlations = df.corr(method='pearson')
        #logger.info(correlations['price actual'].sort_values(ascending=False).to_string())
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

        # Set Figure Size
        return correlations

    @task
    def data_exploration(df):
        df.info()
        df.describe()
        return df

    @task
    def correlation(df):
        df.info()
        df.describe()
        return df

    @task
    def writeDataToSQL(tableName, df):
        logger = get_run_logger()
        logger.info("Writing dataset %s to SQL", tableName)
        engine = create_engine('sqlite:///energystorm.sqlite')
        df.to_sql(tableName, engine, if_exists='replace',index=False)