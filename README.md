## Hathitrust Feature Lemmatised MariaDB
This repository contains code to generate a MariaDB database out of the Hathitrust Extracted Features dataset. It also uses the MorphAdorner software to (1) identify OCR errors and exclude them from the database and (2) provide standardized spelling and lemmata for the remaining tokens.

You will need to have the following software installed in order for this to work:
```
Java
Scala
sbt
MariaDB
```
## Instructions for use
Before running the software make sure you unzip the morphadorner-2.0.1.zip in the morphadorner folder.

1) Download the hathitrust filelist: rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt .

2) Run the scala code to build the MariaDB, remove non-English data and OCR errors, and add in lemmata: sbt -J-d64-Xmx=8G run

## Example query
This query gets a list of all the past tense clauses by lemma, standardized spelling, place of publication and year of publication:
```
SELECT lemmata.lemma lemma, lemmata.standard standard, 
	   metadata.pubDate year, metadata.pubPlace place, 
	   SUM(forms.count) count 
	FROM (forms LEFT JOIN metadata ON forms.volumeID = metadata.id) 
		  LEFT JOIN lemmata 
		  ON forms.lemmaID = lemmata.id 
		  WHERE forms.POS = 'VBD'
		  GROUP BY lemmata.lemma, lemmata.standard, 
		  		   metadata.pubDate, metadata.pubPlace;
```
## Structure of the output database
Three output tables: forms, lemmata, metadata

### Forms Structure
```
form = string containing the raw text from the volume
count = number of attestations on a page
POS = Penn Treebank Part of Speech tag (INDEXED)
pageNum = page number from volume
volumeID = lookup key for the metadata table (INDEXED)
lemmaID = lookup key for the lemmata table (INDEXED)
```
### Lemmata structure
```
id = lookup key for connecting to forms table
lemmaKey = raw text:::lemma part of speech:::general part of speech:::corpus
lemma = the lemma from MorphAdorner
standard = the standardized form from MorphAdorner
```
### Metadata structure
```
id = lookup key for connecting to the forms table
volumeId = Hathitrust volume identifier
title = string containing the title of the volume
pubDate = year of publication
pubPlace = three character code indicating place where volume was published
imprint = strint containing publication information
genre = a set of genres associated with the volume
names = list of names associated with the volume (authors/editors) separated by semicolons
```
## Structure of scala code
The Scala code uses the Akka Actors system to implement a multithreaded system for building the database. 

### Akka structure
The Actor system is built out of individual "actors" each of which runs on its own thread and communicates with other actors via messages. Here is a diagram of the core set of actors in this code:

Main dispatcher -> data workers -> lemma dispatcher -> lemma workers
The main dispatcher's job is to send batches of files to the Downloader to get from the Hathitrust server and send each file to a data worker (the data workers send a message to the dispatcher when they are ready for another file). 

The data workers check if the file is in English and if it is: (i) inserts the metadata into the database, and (ii) inserts all of the individual forms into the database. While it is inserting the forms, it sends "lemmaKeys" (see description in the lemmata table structure) to the lemma dispatcher to be processed

The lemma dispatcher's job is to take lemma keys from the data workers, check if they have already been worked on (by keeping a Set of all worked on keys), and sending new keys off to lemma workers

The lemma workers use MorphAdorner to determine the lemma and standard form for a lemmaKey. If no lemma can be found, don't add anything to the lemmata table. Otherwise, add the new lemma and standard form to the lemmata table.

### Ancilliary Actors

Downloader

Logger

DBRouters (One each for Metadata, Lemmata, Forms) -> DBWriter

The Downloader is given batches of files by the MainDispatcher and rsyncs them from the Hathitrust server

The Logger asynchronously receives timestamped material from the other actors and writes them to a log file (log.csv) with the columns: processname (which actor), workerid (to distinguish different copies of the same actor type), action (log entry type) and timestamp (Unix epoch code)

The DBRouters receive insert queries from the other actors and store them to generate batch queries to send off to the DBWriters who actually send the queries to the database.

### Work flow
1) Load files in batches

2) Process files using data workers

3) Process lemmata using lemma workers

4) When all the files have been processed tell lemma workers to write out any remaining lemmata

5) After all lemmata have been written, normalize the database by creating the form table by (i) removing all forms that don't have a corresponding row in the lemmata table and (ii) inserting the proper integer lookup keys rather than string lookup keys

6) Index the relevant columns in the forms table


