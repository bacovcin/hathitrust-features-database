import glob
from _mysql_exceptions import *
from string import Template
from time import perf_counter as perfc
from bson.objectid import ObjectId
from itertools import islice
import os
import multiprocessing as mp
import sys
import ujson
import bz2
import MySQLdb as mariadb

def create_pos_dict():
    # Load in a list of Penn Treebank POS tags and create a dictionary
    # converting them to word classes for MorphAdorner
    posdict = {}
    infile = open('morphadorner/src/edu/northwestern/at/morphadorner/corpuslinguistics/partsofspeech/resources/penntreebanktags.properties')
    tag = ''
    lemmawc = ''
    majorwc = ''
    wc = ''
    for line in infile:
        if '=' in line:
            s = line.rstrip().split('=')
            if s[0].split('.')[1] == 'name':
                tag = s[1]
            if s[0].split('.')[1] == 'lemmawordclass':
                lemmawc = s[1]
            if s[0].split('.')[1] == 'majorwordclass':
                majorwc = s[1]
            if s[0].split('.')[1] == 'wordclass':
                wc = s[1]
        else:
            if tag != '' and lemmawc != '' and majorwc != '' and wc != '':
                posdict[tag] = (lemmawc,majorwc,wc)
            tag = ''
            lemmawc = ''
            majorwc = ''
            wc = ''
    return posdict

def lemma_worker(queue, debug, batchSize):
    # Open the MariaDB database
    db = mariadb.connect(user='hathitrust',
                         password='plotkin', 
                         database='hathitrust')

    lemmaKeySet = set()

    lemmatains = """INSERT INTO lemmata (lemmaKey)
                             VALUES
                        """
    lemmainstemp = Template("('$lemmaKey')")

    batchcount = 0

    print('Lemma Worker ' + str(os.getpid()) + ' has been initialized.')

    while True:
        # Get the next lemma key
        lemmaKey = queue.get(True)

        if lemmaKey in lemmaKeySet:
            # Already added to database
            queue.task_done()
        else:
            # Insert lemmaKey into set
            lemmaKeySet.add(lemmaKey)
            # Add new command
            db.query(lemmatains + lemmainstemp.substitute(lemmaKey =
                                                          lemmaKey))
            # Increase batchcount
            batchcount += 1
            # Run batch iff batchcount is high enough

            if (batchcount == batchSize):
                if debug:
                    print('Lemma Worker ' + str(os.getpid()) + ' has sent a batch.')
                db.commit()
            queue.task_done()

def worker_init(queue, lemmaqueue, debug):
    # Open the MariaDB database
    db = mariadb.connect(user='hathitrust',
                         password='plotkin', 
                         database='hathitrust')

    # Initialize the POS dictionary
    posdict = create_pos_dict()

    # Initialize list of requests for later bulk insertion
    tokensins = """INSERT INTO tokens
                             (form,
                              count,
                              POS,
                              volumeId,
                              pageNum,
                              lemmaKey)
                             VALUES
                        """

    # Create the format strings for creating insert queries
    tokeninstemp = Template("('$form',$count,'$POS','$volumeIdentifier',$pageNum,'$lemmaKey'),")

    print('Worker ' + str(os.getpid()) + ' has been initialized.')

    while True:
        # Wait until there is an volume in the queue to be processed
        volname = queue.get(True)

        if debug:
            # Output stuff
            print('Worker ' + str(os.getpid()) + ' has loaded ' + volname)
            print('Worker ' + str(os.getpid()) + ' has queue size of ' + 
                  str(queue.qsize()))

        # Load the volume
        volfile = ujson.loads(bz2.open(volname).readline())
        curmeta = volfile['metadata']

        if curmeta['language'] == 'eng':
            # Extract volume level characteristics
            volID = curmeta['volumeIdentifier']
            year = int(curmeta['pubDate'])

            # Add metadata to metadata collection
            metadatains = """INSERT INTO metadata
                             (volumeId,title,pubDate,pubPlace,imprint,genre,names)
                             VALUES
                              ("""
            metadatains += str("'"+volID.replace("'","''")+
                               "','"+curmeta['title'].replace("'","''")+
                               "',"+str(year)+
                               ",'"+curmeta['pubPlace']+
                               "','"+curmeta['imprint'].replace("'","''")+
                               "',('"+','.join(curmeta['genre'])+
                               "'),'"+
                               ';'.join(curmeta['names']).replace("'","''")+
                               "')"
                            )
            db.query(metadatains)

            # Determine what MorphAdorner corpus the volume falls in
            if year < 1700:
                corpus = 'eme' # Early Modern English
            elif year < 1800:
                corpus = 'ece' # Eighteenth Century English
            else:
                corpus = 'ncf' # Nineteenth Century Fiction

            # Create list of documents
            documents = [{'form': y.replace("'","''"),
                          'count': str(x['body']['tokenPosCount'][y][z]),
                          'POS':z.replace("'","''"),
                          'volumeIdentifier':volID,
                          'pageNum':str(int(x['seq'])),
                         }
                         for x in volfile['features']['pages']
                         for y in x['body']['tokenPosCount']
                         for z in x['body']['tokenPosCount'][y]
                        ]

            tokeninserts = []

            for i in range(len(documents)):
                # Get each document
                doc = documents[i]

                # Backslashes cause problems, so skip them
                if "\\" in doc['form']:
                    continue

                # Strip leading characters from POS tags
                if doc['POS'] != '$':
                    doc['POS'] = doc['POS'].lstrip('$')

                try:
                    # Lookup MorphAdorner wordclasses
                    wc = posdict[doc['POS']]

                    # Save wordclasses in the documents lemmaKey
                    doc['lemmaKey'] = doc['form']+':::'+wc[1]+':::'+wc[0]+':::'+corpus
                    lemmaqueue.put(doc['lemmaKey'])

                    # Add the document as a request to the list
                    # of tokens requests
                    tokeninserts.append(tokeninstemp.substitute(doc))
                except KeyError:
                    if debug:
                        with open('errorlist.txt','a') as errorfile:
                            errorfile.write(str(doc)+'\n')
                    continue
            curinsert = ''
            i = 0
            for ins in tokeninserts:
                if i == 99:
                    db.query(tokensins+curinsert[:-1])
                    curinsert = ''
                    i = 0
                curinsert += ins
            else:
                db.query(tokensins+curinsert[:-1])
            db.commit()
        os.remove(volname)
        queue.task_done()

def main():
    # Temporarily open the MongoDB database
    try:
        db = mariadb.connect(user='hathitrust',
                             password='plotkin', 
                             database='hathitrust')
    except:
        dbclient = mariadb.connect(user='hathitrust',
                                   password='plotkin')
        dbclient.query('CREATE DATABASE hathitrust')
        dbclient.close()
        db = mariadb.connect(user='hathitrust',
                             password='plotkin', 
                             database='hathitrust')
    db.query('DROP TABLE IF EXISTS tokens')
    db.query('DROP TABLE IF EXISTS lemmata')
    db.query('DROP TABLE IF EXISTS metadata')
    db.query('''CREATE TABLE tokens (
                    form varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                    count int NOT NULL,
                    POS varchar(4) NOT NULL,
                    volumeId varchar(255) NOT NULL,
                    pageNum int NOT NULL,
                    lemmaKey varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL
                )''')
    db.query('''CREATE TABLE lemmata (
                    id int NOT NULL UNIQUE AUTO_INCREMENT,
                    lemmaKey varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                    lemma varchar(255) CHARACTER SET utf8 COLLATE utf8_bin,
                    standard varchar(255) CHARACTER SET utf8 COLLATE utf8_bin,
                    PRIMARY KEY (id)
                )''')
    db.query("""CREATE TABLE metadata (
                    id int NOT NULL UNIQUE AUTO_INCREMENT,
                    volumeId varchar(100) NOT NULL,
                    title varchar(255) NOT NULL,
                    pubDate int NOT NULL,
                    pubPlace varchar(3) NOT NULL,
                    imprint varchar(255) NOT NULL,
                    genre  set('not fiction',
                               'legal case and case notes',
                               'government publication',
                               'novel',
                               'drama',
                               'bibliography',
                               'poetry',
                               'biography',
                               'catalog',
                               'fiction',
                               'statistics',
                               'dictionary',
                               'essay',
                               'index') NOT NULL,
                    names varchar(255) NOT NULL,
                    PRIMARY KEY (id)
                )""")

    db.query("SET GLOBAL max_allowed_packet=1073741824")

    db.commit()
    db.close()

    # Set defaults for flags
    tmps = 200
    downsize = 1000
    batchSize = 100
    debug=False
    poolsize=5
    nofilelist=False

    # Set flags if passed as arguments
    for x in sys.argv:
        try:
            s = x.split('=')
            if s[0] == 'downsize':
                try:
                    downsize = int(s[1])
                except ValueError:
                    raise("Your downsize value must be an integer.")
            elif s[0] == 'tmps':
                try:
                    tmps = int(s[1])
                except ValueError:
                    raise("Your tmps value must be an integer.")
            elif s[0] == 'batchSize':
                try:
                    batchSize = int(s[1])
                except ValueError:
                    raise("Your batchSize value must be an integer.")
            elif s[0] == 'poolsize':
                try:
                    poolsize = int(s[1])
                except ValueError:
                    raise("Your poolsize value must be an integer.")
            elif s[0] == 'debug':
                debug = True
            elif s[0] == 'nofilelist':
                nofilelist = True
        except:
            continue

    # Create the worker pool for document processing
    document_queue = mp.JoinableQueue()
    lemmakey_queue = mp.JoinableQueue()
    mylemmatracker = mp.Pool(1,
                             lemma_worker, 
                             (lemmakey_queue,
                              debug,
                              batchSize,))
    mypool = mp.Pool(poolsize - 1, worker_init,(document_queue,
                                                lemmakey_queue,
                                                debug,))

    if not nofilelist:
        # Make sure that the file list is up to date
        os.system("rsync -azv " +
                  "data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt" +
                  " .")

    # Reset downsize to be number of volumes if downsize = -1
    vols = open('htrc-ef-all-files.txt')
    if downsize == -1:
        downsize = sum(1 for line in vols)
    vols.close()

    # Open the list of all htrc volumes
    vols = open('htrc-ef-all-files.txt')

    # Create tracker for number of files to download
    i = 0
    while i < downsize:
        print('Number of files loaded: '+str(i+1))

        # Create a file with the next set of volumes
        volsamples = list(islice(vols, tmps))
        i += tmps
        outfile = open('sample.txt','w')
        outfile.write(''.join(volsamples))
        outfile.close()

        # Download files as tmp0-tmpX, overwriting previous tmp files
        if debug:
            os.system("rsync -av --no-relative --files-from sample.txt " +
                      "data.analytics.hathitrust.org::features/ .")
        else:
            os.system("rsync -av --no-relative --files-from sample.txt " +
                      "data.analytics.hathitrust.org::features/ . >/dev/null 2>&1")


        # Wait until all of the documents have been processed
        document_queue.join()

        # Get the paths to all the tmp files
        tmpfiles = glob.glob('*.bz2')

        # Put the current set of documents on the queue for processing
        for f in tmpfiles:
            document_queue.put(f)

    document_queue.join()
    lemmakey_queue.join()
    # After all documents have been added create indices for database
    db = mariadb.connect(user='hathitrust',
                         password='plotkin', 
                         database='hathitrust')
    db.commit()
    print("Requiring metadata to have unique volumeId...")
    db.query("ALTER TABLE metadata ADD UNIQUE metadata(volumeId)")
    print("Indexing lemmaKey in tokens for future work...")
    db.query('CREATE INDEX lemmaKey ON tokens(lemmaKey)')
    db.commit()
    db.close()

if __name__ == "__main__":
    main()
