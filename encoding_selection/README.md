# Data-Driven Encoding Selector
This project explores an automated method to select efficient lightweight encoding schemes for column-based databases, such as run length and bitmap encoding. Comparing to popular compression techniques like gzip and snappy, lightweight encoding has the following benefits:
* Speed: Lightweight encodings involve only simple operations and usually has negligible impact on data access time. 
* Local Computation: Most Lightweight encoding schemes only access adjacent local tuples during encoding/decoding process, without the need to go through entire dataset.
* In-Site Query Execution: Encoding schemes enable in-place query execution that allows queries to be directly applied on encoded data and further reduce the time needed for query processing, instead of requiring a large block of data to be uncompressed first.

However, there is no simple rule on how to choose the best encoding scheme for a given attribute. Existing systems either rely on database administrators' personal experience or use simple rules to make selection. In practice, neither of these two methods achieve optimal performance. Our method attacks the problem in a systematic data-driven way. Our effort includes:
* Dataset Collection
* Pattern Mining
* Data-Driven Encoding Prediction
 
## Dataset Collection
Our work is based on analysis and evaluation of real-world datasets. We have created an automated framework to collect datasets, extract columns from them, organize, and persist the records for further analysis. The framework could accept various input formats including csv, txt, JSon and MS Excel files. It also supports recognition of column data type for unattended data collection. For further analysis purpose, the framework provides API enabling customized features to be extracted from the columns.

Using this framework, we have collected over 7000 columns from approximately 1200 datasets with a total size of 500G data. These datasets are all from real-world data sources and cover a rich collection of data types (integer, date, address, etc.), with diverse data distributions. We use Apache Parquet's built-in encoders to encode these data columns with different encoding schemes, looking for the one performing best for each column. We also developed some customized encoders to compare their performance.

[Insert some charts we generated for the dataset]

Here's a list of data sources we used in the experiments.
* Government Data Portals
  * [Open Government Data](https://www.data.gov/open-gov/)
  * [NYC](https://opendata.cityofnewyork.us/)
  * [Chicago](https://data.cityofchicago.org/)
  * [Baltimore](https://data.baltimorecity.gov/)
  * [Dallas](https://mydata.dallasisd.org/)
  * [Washington D.C.](https://dc.gov/page/open-data)
  * [LA](https://data.lacity.org/)
  * [Maryland](https://data.maryland.gov/)
  * [Lousiville](https://data.louisvilleky.gov/)
  * [Oakland](https://data.oaklandnet.com/)
* Server Logs
  * Argonne Server Logs [Requesting permission to publish]
* GIS data
  * [ArcGIS Open Data](http://hub.arcgis.com/pages/open-data)
  * [Esri Open Data](http://www.esri.com/software/open/open-data)
* Social Networks
  * [Bikeshare](https://www.bikeshare.com/)
  * [Yelp](https://www.yelp.com/dataset/challenge)
* Machine Learning Datasets
  * [UCI Repo](https://archive.ics.uci.edu/ml/)


## Data Driven Encoding Prediction

In this project, we target at building a encoding selector that is able to "predict" the best encoding scheme by looking at only a limited section of the entire dataset, e.g., the first 1% of the records. To do this, we plan to use the collected data columns and the "ground truth" best encoding scheme to train a neural network for this purpose. 

Choosing core features that truly represents the relationship between data and label is crucial to the success of neural network training task. The features we choose including statistical information such as mean and variance of data length, entropy. 


## Build the Project

Run `mvn clean package` to build the project and run unit test
