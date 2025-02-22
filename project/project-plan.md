
# Project Plan

## Weathering the Storm: Forecasting Energy Consumption and Pricing Trends Amidst Changing Climate Conditions. 
<!-- Give your project a short title. -->
Correlation Between Energy and Weather to anticipate energy demand surges and dips.

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. Identify weather patterns and climate factors that significantly impact energy consumption to anticipate energy demand surges and dips.
2. Insights into the relationship between energy consumption, pricing, and climate patterns.

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->

In a rapidly changing climate, accurate energy demand and pricing forecasts are crucial for ensuring grid stability and sustainable resource management. This project aims to develop a sophisticated machine learning model that predicts energy consumption and pricing trends.

The model will analyze historical energy consumption data, weather patterns, and climate projections to identify key relationships driving energy demand and pricing dynamics. This will enable utilities to:

* Proactively prepare for demand fluctuations caused by extreme weather events.

* Optimize energy pricing strategies to ensure financial sustainability while maintaining affordability for consumers.

* Effectively integrate renewable energy sources into the grid, promoting a sustainable energy future.

By harnessing the power of machine learning, this project will equip stakeholders with the knowledge and tools to navigate the evolving energy landscape and ensure energy security for all.


## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Energy Data
* Metadata URL: https://transparency.entsoe.eu/
* Data URL: https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=08.11.2023+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=08.11.2023+00:00|CET|DAYTIMERANGE&area.values=CTY|10Y1001A1001A83F!BZN|10Y1001A1001A82H&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B07&productionType.values=B08&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B20&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)#
* Data Type: CSV

This dataset contains yearly electrical consumption, generation data for european countries. Consumption and generation data was retrieved from ENTSOE a public portal for Transmission Service Operator (TSO) data. 


### Datasource1: Weather
* Metadata URL: https://openweathermap.org/api
* Data URL: https://openweathermap.org/bulk#
* Data Type: CSV

Current, forecast and historical weather data is available via regulary updated JSON or CSV files. In order to receive this data, you will need to download an archive via provided link. Each archive will contain selected weather data by chosen locations lists.


## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Explore Datasources [https://github.com/srinivasseema/made-template/issues/1]
2. Analyze data pipeline requirements
