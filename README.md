## Hathitrust Feature Aggregator using Apache Spark

This code repository contains code for using Apach Spark to combine resources from the [HTRC Extracted Features dataset](https://wiki.htrc.illinois.edu/display/COM/Extracted+Features+Dataset) using [htrc-feature-reader](https://pypi.python.org/pypi/htrc-feature-reader).

In order for the code to work, you need to download the list of volumes from Hathitrust:

rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt .

