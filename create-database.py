from py4j.java_gateway import JavaGateway
import glob
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
     # Opoen the MongoDB database
    dbclient = pymongo.MongoClient()
    db = dbclient.hathitrust

    # Create lemmatiser connection
    gateway = JavaGateway()
    lemmatiser = gateway.entry_point.getLemmatiser()

    # Initialize teh POS dictionary
    posdict = create_pos_dict()

    print('Worker ' + str(os.getpid()) + ' has been initialized.')

    while True:
        # Wait until there is an volume in the queue to be processed
        volname = queue.get(True)

        # Output stuff
        print('Worker ' + str(os.getpid()) + ' has loaded ' + volname)

        if debug:
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

            # Extract all the words in the body of the text
            dLbody = [(y,
                       x['body']['tokenPosCount'][y],
                      int( x['seq']))
                      for x in volfile['features']['pages']
                      for y in x['body']['tokenPosCount']
                     ]

            # Open file for transfer of data to MorphAdorner
            outfilename = str(os.getpid())+'-input.csv'
            outfile = open(outfilename,'w')

            # Preprocess each word and put it in the database
            for word in dLbody:
                for key in word[1]:
                    try:
                        # Convert Hathitrust POS into format for MorphAdorner
                        if key != '$':
                            pos = key.lstrip('$')
                        else:
                            pos = key
                        # Get the word class of the word
                        wc = posdict[pos]

                        outfile.write(word[0]+'\t'+
                                      wc[1]+'\t'+
                                      wc[2]+'\t'+
                                      key+'~='+str(word[1][key])+'\t'+
                                      str(word[2])+'\n')
                    except KeyError:
                        continue

            # Send request to MorphAdorner to lemmatise file
            if corpus == 'eme':
                infilename = lemmatiser.adornEmeFile(outfilename)
            elif corpus == 'ece':
                infilename = lemmatiser.adornEceFile(outfilename)
            elif corpus == 'ncf':
                infilename = lemmatiser.adornNcfFile(outfilename)

            # Open results to send to MongoDB
            infile = open(infilename)

            # Read through MorphAdorner output and 
            # create the documents for MongoDB
            documents = []
            for line in infile:
                s = line.rstrip().split('\t')
                try:
                    poss = s[3].split('~=')
                    documents.append({
                                     'form':s[0],
                                     'standardSpelling':s[1],
                                     'lemma':s[2],
                                     'POS':poss[0],
                                     'count':int(poss[1]),
                                     'volumeIdentifier':volID,
                                     'pageNum':s[4]
                                    })
                except IndexError:
                    # Something has gone wrong and there is an line with the 
                    # wrong format (save the line to the error file and
                    # continue)
                    with open('errorline.txt','a') as errorfile:
                        errorfile.write(line)
                    continue

            # Bulk insert documents into mongodb
            db.tokens.insert_many(documents,
                                  ordered=False,
                                  bypass_document_validation=True
                                 )
        os.remove(volname)
        queue.task_done()

def main():
    # Temporarily open the MongoDB database
    with pymongo.MongoClient() as dbclient:
        db = dbclient.hathitrust

        # Reinitialize the collections to be rebuilt
        db.tokens.drop()
        db.metadata.drop()

    # Set defaults for flags
    tmps = 1
    downsize = 1
    debug=False
    poolsize=1

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
    os.system("rsync -az " +
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
    # After all documents have been added create indeices for database
    db.tokens.create_index([('lemma',pymongo.ASCENDING),
                            ('POS',pymongo.ASCENDING)])
    db.metadata.create_index('volumeIdentifier')

    # Deleted temporary files used to interface with MorphAdorner
    os.remove(glob.glob("*input.csv"))
    os.remove(glob.glob("*output.txt"))

if __name__ == "__main__":
    main()
