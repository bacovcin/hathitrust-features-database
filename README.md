### Hathitrust Feature Lemmatised MongoDB
This repository contains code to generate a MongoDB database out of the Hathitrust Extracted Features dataset. It also uses the MorphAdorner software to (1) identify OCR errors and exclude them from the database and (2) provide standardized spelling and lemmata for the remaining tokens.

You will need to have the following software installed in order for this to work:
Java
Scala
sbt
Python
pymongo
MongoDB

## Instructions for use
#Before running the software make sure you unzip the morphadorner-2.0.1.zip in the morphadorner folder.

1) Run create-database.py to download the Hathitrust data and add it to the MongoDB database: python create-database.py
2_ Run the scala code to lemmatise the MongoDB and remove non-English data and OCR errors:
	a) cd lemmatiser-code/lemmatiser
	b) sbt
	c) In the sbt console type: run
