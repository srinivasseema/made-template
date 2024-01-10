from energypipeline import EnergyDataPipeline
from prefect import flow, task


@flow
def energyDataFlow():
    pipeline = EnergyDataPipeline()
    #make pipeline sections
    #pipeline.downloadfiles()
    energyDF = pipeline.readEnergyData()
    pipeline.data_exploration(energyDF)
    pipeline.null_values("energy", energyDF)
    correlations = pipeline.findCorrelations(energyDF)
    wrangledData = pipeline.wrangleData(energyDF)
    print(wrangledData.head(5))
    pipeline.pricePerTotalLoad(wrangledData)
    pipeline.splitDataAndBaseline(wrangledData)
    pipeline.writeDataToSQL("energy", energyDF)

@flow
def weatherDataFlow():
    pipeline = EnergyDataPipeline()
    #make pipeline sections
    #pipeline.downloadfiles()
    weatherDF = pipeline.readWeatherData()
    pipeline.data_exploration(weatherDF)
    #pipeline.null_values("weather", weatherDF)
    #correlations = pipeline.findCorrelations(weatherDF)
    #pipeline.plotHeatmap(correlations)
    pipeline.writeDataToSQL("weather", weatherDF)
    #Found very high correlation between some columns.
    #All data seems to be numeric.

@flow
def executePipeline():
    energyDataFlow()
    weatherDataFlow()


@flow
def testFlow():
    print("Hello world")

if __name__ == "__main__":
    executePipeline()