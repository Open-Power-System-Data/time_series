# About Open Power System Data 
This notebook is part of the project Open Power System Data. Open Power System Data develops a platform for free and open data for electricity system modeling. We collect, check, process, document, and provide data that are publicly available but currently inconvenient to use. 
More info on Open Power System Data:
- [Information on the project on our website](http://open-power-system-data.org)
- [Data and metadata on our data platform](http://data.open-power-system-data.org)
- [Data processing scripts on our GitHub page](https://github.com/Open-Power-System-Data)

# About Jupyter Notebooks
A Jupyter Notebook is a file that combines executable programming code with visualizations and comments in markdown format, allowing for an intuitive documentation of the code. We use these as a single file for coding and documentation. [More info on our IT-concept](http://open-power-system-data.org/it)

# About this datapackage
The [timeseries-datapackage](http://data.open-power-system-data.org/datapackage_timeseries/) contains different kinds of timeseries data relevant for power system modelling, namely:
- electricity consumption (load)
- wind and solar power generation and available capacities
- prices
The main focus of this datapackage is German data, but we include data from other countries wherever possible.
The timeseries become available at different points in time depending on the sources. The full dataset is only available from 2012 onwards.
The data has been downloaded from the sources, resampled and merged in a large CSV file with hourly resolution. Additionally, the data available at a higher resolution (Some renewables in-feed, 15 minutes) is provided in a separate file.

# Data sources
The main data sources are the various European Transmission System Operators (TSOs) and the ENTSO-E Data Portal. A complete list of data sources is integrated in the Field Documentation of the [timeseries-datapackage](http://data.open-power-system-data.org/datapackage_timeseries/) 

# Notation

[Table with notation and abbreviations] 

    {
     "data": {
      "text/html": [
       "<div>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>term</th>\n",
       "      <th>meaning</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>load</td>\n",
       "      <td>Consumption in MW</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>price</td>\n",
       "      <td>day-ahead price</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>generation</td>\n",
       "      <td>electricity produced</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>forecast</td>\n",
       "      <td>day-ahead forecast</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>capacity</td>\n",
       "      <td>installed capacity</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>solar</td>\n",
       "      <td>photovoltaics</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>wind-onshore</td>\n",
       "      <td>wind-onshore</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>wind-offshore</td>\n",
       "      <td>wind-offshore</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>BE</td>\n",
       "      <td>ISO-2 digit country code for Belgium</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "            term                               meaning\n",
       "0           load                     Consumption in MW\n",
       "1          price                       day-ahead price\n",
       "2     generation                  electricity produced\n",
       "3       forecast                    day-ahead forecast\n",
       "4       capacity                    installed capacity\n",
       "5          solar                         photovoltaics\n",
       "6   wind-onshore                          wind-onshore\n",
       "7  wind-offshore                         wind-offshore\n",
       "8             BE  ISO-2 digit country code for Belgium"
      ]
     }

# License
This notebook as well as all other documents in this repository is published under the [MIT License](https://opensource.org/licenses/MIT).

# Links to the other notebooks of this package
The download and read functions are implemented as distinct modules that are imported to this notebook. Click below to inspect the code (The link to the local copy will only work if you are running this notebook on your computer):

- The [**processing notbook**](processing.ipynb) handles misssing and implausible data, performs calculations and aggragations and creates the output files. 
- The [**sources file**](config/sources.yml) contains for each data source the specific information necessary to access and process the data from that source. 
- The [**download script**](timeseries_scripts/download.py) downloads the data from the sources to your hard drive.
- The [**read script**](timeseries_scripts/read.py) reads each downloaded file into a pandas-DataFrame and merges data from different sources but with the same time resolution.

If you are viewing this file on your computer, you can also go to  [/timeseries_scripts](timeseries_scripts) to see the above files with syntax highlighting.