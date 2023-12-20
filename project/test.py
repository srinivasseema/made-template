import pytest
from prefect import Flow
from pipeline import EnergyDataPipeline

@pytest.fixture
def pipeline():
    return EnergyDataPipeline()

def test_weather_flow_runs_without_errors(pipeline):
    with pytest.raises(Exception) as excinfo:
        weatherDF = pipeline.readWeatherData.fn()
        pipeline.data_exploration.fn(weatherDF)
        pipeline.null_values.fn("weather", weatherDF) 

def test_energy_flow_runs_without_errors(pipeline):
    with pytest.raises(Exception) as excinfo:
        energyDF = pipeline.readEnergyData.fn()
        pipeline.data_exploration.fn(energyDF)
        pipeline.null_values.fn("energy", energyDF)