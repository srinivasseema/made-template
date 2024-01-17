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

@flow(log_prints=True)
def executePipeline():
    """
    Executes the downloadfilesAndExplore and energy_data_flow flows.
    """
    downloadfilesAndExplore()
    energy_data_flow()
    

if __name__ == "__main__":
    executePipeline.serve(name="energy-data-flow")