# UK House Prices on Map

The goal of the project is to provide data analysts (e.g. government statisticians preparing annual reports, business analytics in real estate companies using dashboards) with a data platform that focuses on time series of prices and geographical location.
For that, we need a dataset of sale transactions and map locations of UK addresses, which we combine from two different open sources.
Since the solution needs to allow end users perform complex queries (e.g. analyses by year and location), and the dataset is being continuously generated with substantial size, AWS Redshift is used as a platform for queries, together with Apache Spark for data processing.

Please see UK_House_Prices_on_Map.ipynb.
