## Hathitrust Feature Lemmatised MongoDB
This repository contains code to generate a MongoDB database out of the Hathitrust Extracted Features dataset. It also uses the MorphAdorner software to (1) identify OCR errors and exclude them from the database and (2) provide standardized spelling and lemmata for the remaining tokens.

You will need to have the following software installed in order for this to work:
Java
Python
pymongo
py4j
MongoDB

# Instructions for use
Before running the software make sure you unzip the morphadorner-2.0.1.zip in the morphadorner folder.

0) If java is causing problems, try recompiling the java code: javac -cp "lemmatisation-server:lemmatisation-server/py4j0.10.6.jar:morphadorner/src/" lemmatisation-server/py4jserver/*.java
1) Start the MorphAdorner py4j server: java -cp "lemmatisation-server:lemmatisation-server/py4j0.10.6.jar:morphadorner/src/" -Xms6g py4jserver.ServerEntryPoint
2) Run create-database.py to download the Hathitrust data and add it to the MongoDB database: python create-database.py
