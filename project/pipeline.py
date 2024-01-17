from energypipeline import EnergyDataPipeline
from prefect import flow
from prefect.deployments import run_deployment
from prefect_jupyter import notebook

@flow
def energy_data_flow():
    """
    Executes a Jupyter notebook, exports it, and saves it to a file.

    Returns:
    output_path (str): The path of the executed notebook file.
    """
    nb = notebook.execute_notebook(
         "examples\energy-data-analysis.ipynb"
    )
    body = notebook.export_notebook(nb)
    output_path = "executed_notebook.ipynb"
    with open(output_path, "w", encoding= "UTF-8") as f:
        f.write(body)
    return output_path

@flow
def downloadfilesAndExplore():
    """
    Downloads files, reads energy data, performs data exploration, and checks for null values.
    """
    pipeline = EnergyDataPipeline()
    #pipeline.downloadfiles()
    energyDF = pipeline.readEnergyData()
    pipeline.data_exploration(energyDF)
    pipeline.null_values_check(energyDF)

""" @flow
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
    pipeline.writeDataToSQL("energy", energyDF) """

""" @flow
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
    #All data seems to be numeric. """

@flow(log_prints=True)
def executePipeline():
    """
    Executes the downloadfilesAndExplore and energy_data_flow flows.
    """
    downloadfilesAndExplore()
    energy_data_flow()
    

@flow
def testFlow():
    """
    Prints "Hello world".
    """
    print("Hello world")

if __name__ == "__main__":
    executePipeline.serve(name="energy-data-flow")
    #executePipeline()