
# coding: utf-8

# # Open Power System Data: time series

# Part of the project [Open Power System Data](http://open-power-system-data.org/).

# # Table of Contents
# * [1. About this notebook](#1.-About-this-notebook)
# * [2. Links to the other notebooks](#2.-Links-to-the-other-notebooks)
# * [3. What is in the output files?](#3.-What-is-in-the-output-files?)
# 	* [3.1 Data sources](#3.1-Data-sources)
# 	* [3.2 Data documentation and interpretation](#3.2-Data-documentation-and-interpretation)
# 

# # 1. About this notebook

# This is the first of 4 Jupyter notebook python scripts that downloads and processes time-series data from European power systems. The notebooks have been used to create the [timeseries-datapackage](http://data.open-power-system-data.org/datapackage_timeseries/) that is available on the [Open Power System Data plattform](http://data.open-power-system-data.org/). A Jupyter notebook is a file that combines executable programming code with visualizations and comments in markdown format, allowing for an intuitive documentation of the code.
# 
# The notebooks are part of a [GitHub repository](https://github.com/Open-Power-System-Data/datapackage_timeseries) and can be [downloaded](https://github.com/Open-Power-System-Data/datapackage_timeseries/archive/master.zip) for execution on your local computer (You need a running python installation to do this, for example [Anaconda](https://www.continuum.io/downloads)).  Executed one after another, they can be used to reproduce the dataset that we provide for download.
# 
# This notebook itself is just an entry point to the other notebooks that explains how they work together.

# # 2. Links to the other notebooks

# The other scripts/notebooks each implement a distinct function (The local copy will only work if you are running this notebook on your yomputer):
# - **The download script** ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/download.ipynb) / [local copy](download.ipynb)) downloads the data from our [sources](http://open-power-system-data.org/opsd-sources#time-series) to your hard drive.
# - **The read script** ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/read.ipynb) / [local copy](read.ipynb)) reads each downloaded file into a pandas-DataFrame and merges data from different sources but with the same time resolution.
# - **The processing script** ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/processing.ipynb) / [local copy](processing.ipynb)) performs some aggregations and transforms the data to the [tabular data package format](http://data.okfn.org/doc/tabular-data-package), where actual data is saved in a CSV file, while metadata (information on format, units, sources, and descriptions) is stored in a JSON file.

# # 3. What is in the output files?

# ## 3.1 Data sources

# An overview of the sources for the data can be found [here](http://open-power-system-data.org/opsd-sources#time-series).

# ## 3.2 Data documentation and interpretation

# Often, the data that we use is poorly documented. In some cases, primary data owners provide some documentation.
# 
# 
# **Load data**
# * [ENTSO-E Specific national considerations](https://www.entsoe.eu/Documents/Publications/Statistics/Specific_national_considerations.pdf)
# * [Schumacher & Hirth 2015](http://papers.ssrn.com/sol3/papers.cfm?abstract_id=2715986), a paper on load data
