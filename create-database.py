import glob
from time import perf_counter as perfc
from bson.objectid import ObjectId
from itertools import islice
import os
import multiprocessing as mp
import sys
import ujson
import bz2
import pymongo

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

def worker_init(queue, debug):
     # Open the MongoDB database
    dbclient = pymongo.MongoClient()
    db = dbclient.hathitrust

    # Initialize the POS dictionary
    posdict = create_pos_dict()

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

        if volfile['metadata']['language'] == 'eng':
            # Extract volume level characteristics
            volID = volfile['metadata']['volumeIdentifier']
            year = int(volfile['metadata']['pubDate'])

            # Add metadata to metadata collection
            db.metadata.insert_one(volfile['metadata'])

            # Determine what MorphAdorner corpus the volume falls in
            if year < 1700:
                corpus = 'eme' # Early Modern English
            elif year < 1800:
                corpus = 'ece' # Eighteenth Century English
            else:
                corpus = 'ncf' # Nineteenth Century Fiction

            # Create list of documents
            documents = [{'form': y,
                          'count': x['body']['tokenPosCount'][y][z],
                          'POS':z,
                          'volumeIdentifier':volID,
                          'pageNum':x['seq'],
                         }
                         for x in volfile['features']['pages']
                         for y in x['body']['tokenPosCount']
                         for z in x['body']['tokenPosCount'][y]
                        ]

            # Initialize list of requests for later bulk insertion
            tokensRequests = []
            lemmataRequests = []

            for i in range(len(documents)):
                # Get each document
                doc = documents[i]

                # Strip leading characters from POS tags
                if doc['POS'] != '$':
                    doc['POS'] = doc['POS'].lstrip('$')

                try:
                    # Lookup MorphAdorner wordclasses
                    wc = posdict[doc['POS']]

                    # Save wordclasses in the documents lemmaKey
                    doc['lemmaKey'] = doc['form']+':::'+wc[1]+':::'+wc[0]+':::'+corpus

                    # Add the document as a request to the list
                    # of tokens requests
                    tokensRequests.append(pymongo.InsertOne(doc))

                    # Add the lemmaKey as a document for lemmata
                    lemmataRequests.append(pymongo.UpdateOne(filter={'_id':doc['lemmaKey']},
                                                             update={'$set':{'_id':doc['lemmaKey']}},
                                                             upsert=True))
                except KeyError:
                    if debug:
                        with open('errorlist.txt','a') as errorfile:
                            errorfile.write(str(doc)+'\n')
                    continue
            # Bulk insert documents into mongodb
            startt = perfc()
            db.tokens.bulk_write(tokensRequests,
                                 ordered=False)
            with open('timings.csv','a') as outfile:
                outfile.write(str(os.getpid())+','+
                              str(perfc() - startt)+','+
                              str(len(tokensRequests))+','+
                              'tokens\n')

            startt = perfc()
            db.lemmata.bulk_write(lemmataRequests,
                                  ordered=False)
            with open('timings.csv','a') as outfile:
                outfile.write(str(os.getpid())+','+
                              str(perfc() - startt)+','+
                              str(len(lemmataRequests))+','+
                              'lemmata\n')

        os.remove(volname)
        queue.task_done()

def main():
    # Temporarily open the MongoDB database
    with pymongo.MongoClient() as dbclient:
        db = dbclient.hathitrust

        # Reinitialize the collections to be rebuilt
        db.tokens.drop()
        db.lemmata.drop()
        db.metadata.drop()

    # Set defaults for flags
    tmps = 200
    downsize = 1000
    debug=False
    poolsize=5

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
            elif s[0] == 'poolsize':
                try:
                    poolsize = int(s[1])
                except ValueError:
                    raise("Your poolsize value must be an integer.")
            elif s[0] == 'debug':
                debug = True
        except:
            continue

    # Create the worker pool for document processing
    document_queue = mp.JoinableQueue()
    mypool = mp.Pool(poolsize, worker_init,(document_queue,debug,))

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
    # After all documents have been added create indices for database
    print("Indexing...")
    db.metadata.create_index('volumeIdentifier')
    db.tokens.create_index('lemmaKey')

if __name__ == "__main__":
    main()
