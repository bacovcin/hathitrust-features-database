### Hathitrust Feature Lemmatised MariaDB
This repository contains code to generate a MariaDB database out of the Hathitrust Extracted Features dataset. It also uses the MorphAdorner software to (1) identify OCR errors and exclude them from the database and (2) provide standardized spelling and lemmata for the remaining tokens.

You will need to have the following software installed in order for this to work:
Java
Scala
sbt
MariaDB

## Instructions for use
Before running the software make sure you unzip the morphadorner-2.0.1.zip in the morphadorner folder.

1) Download the hathitrust filelist: rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt .
2_ Run the scala code to build the MariaDB, remove non-English data and OCR errors, and add in lemmata: sbt -J-xms=8gb run

### Example query
This query gets a list of all the past tense clauses by lemma, standardized spelling, place of publication and year of publication:
SELECT lemmata.lemma lemma, lemmata.standard standard, metadata.pubDate year, metadata.pubPlace place, SUM(forms.count) count 
	FROM (forms 
			LEFT JOIN metadata ON forms.volumeID = metadata.id) 
		LEFT JOIN lemmata 
		ON forms.lemmaID = lemmata.id 
		WHERE forms.POS = 'VBD'
		GROUP BY lemmata.lemma, lemmata.standard, metadata.pubDate, metadata.pubPlace;

### Structure of the output database
Three output tables: forms, lemmata, metadata

## Forms Structure
form = string containing the raw text from the volume
count = number of attestations on a page
POS = Penn Treebank Part of Speech tag (INDEXED)
pageNum = page number from volume
volumeID = lookup key for the metadata table (INDEXED)
lemmaID = lookup key for the lemmata table (INDEXED)

## Lemmata structure
id = lookup key for connecting to forms table
lemmaKey = raw text:::lemma part of speech:::general part of speech:::corpus
lemma = the lemma from MorphAdorner
standard = the standardized form from MorphAdorner

## Metadata structure
id = lookup key for connecting to the forms table
volumeId = Hathitrust volume identifier
title = string containing the title of the volume
pubDate = year of publication
pubPlace = three character code indicating place where volume was published
imprint = strint containing publication information
genre = a set of genres associated with the volume
names = list of names associated with the volume (authors/editors) separated by semicolons

### Structure of scala code
The Scala code uses the Akka Actors system to implement a multithreaded system for building the database. The Actor system is built out of individual "actors" each of which runs on its own thread and communicates with other actors via messages. Here is a diagram of the set of actors in this code:

Main dispatcher -> data workers -> lemma dispatcher -> lemma workers

The main dispatcher's job is to download the data in batches and send each file to a data worker (the data workers send a message to the dispatcher when they are ready for another file). 

The data workers check if the file is in English and if it is: (i) inserts the metadata into the database, and (ii) inserts all of the individual forms into the database. While it is inserting the forms, it sends "lemmaKeys" to the lemma dispatcher to be processed
