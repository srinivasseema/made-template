import unittest
from prefect import Flow
from pipeline import EnergyDataPipeline
import logging

class TestEnergyDataPipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = EnergyDataPipeline()

    def test_weather_flow_runs_without_errors(self):
        try:
            weatherDF = self.pipeline.readWeatherData.fn()
            self.pipeline.data_exploration.fn(weatherDF)
            self.pipeline.null_values.fn("weather", weatherDF)
        except Exception as e:
            logging.error(f"Error running Weather Flow: {e}")
            self.fail(f"Flow raised an exception: {e}")

    def test_energy_flow_runs_without_errors(self):
        try:
            energyDF = self.pipeline.readEnergyData.fn()
            self.pipeline.data_exploration.fn(energyDF)
            self.pipeline.null_values.fn("energy", energyDF)
        except Exception as e:
            logging.error(f"Error running Energy Flow: {e}")
            self.fail(f"Flow raised an exception: {e}")

if __name__ == "__main__":
    unittest.main()
