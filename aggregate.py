import glob
import os
import sys
import pandas as pd
import pyrsync2 as rsync
from htrc_features import FeatureReader
from htrc_features import utils as htrcutils
from pyspark import SparkContext

def extract_database(vol, pos, lang):
    '''Function for mapping htrc_features volumes to a list of tuples with
    the following structure ((word, pos, year, place, genre), count)'''

    # Extract volume information to save on lookups
    year = vol.year
    pp = vol.pub_place
    gen = vol.genre

    # Only extract English data
    if vol.language == lang or lang == '':
        if pos != '':
            return [((x[1],x[2],vol.year,vol.pub_place,vol.genre[0]),x[3])
                    for x in vol.tokenlist(pages=False,
                                           case=False,
                                           pos=True).filter(like=pos,axis=0).to_records()]
        else:
            return [((x[1],x[2],vol.year,vol.pub_place,vol.genre[0]),x[3])
                    for x in vol.tokenlist(pages=False,
                                           case=False,
                                           pos=True).to_records()]
    else:
        return []

if __name__ == "__main__":
    # Remove environment variables that can cause errors
    del os.environ['PYSPARK_SUBMIT_ARGS']

    # Set defaults for flags
    pos = ''
    lang = 'eng'
    iters = range(5)
    tmps = 20

    # Set flags if passed as arguments
    for x in sys.argv:
        try:
            s = x.split('=')
            if s[0] == 'pos':
                pos = s[1]
            elif s[0] == 'iters':
                iters = range(int(s[1]))
            elif s[0] == 'tmps':
                tmps = int(s[1])
            elif s[0] == 'lang':
                lang = s[1]
        except:
            continue

    print('POS: '+pos+'\tIters: '+str(iters)+'\tTmps: '+
          str(tmps)+'\tLang: '+lang+'\n')

    # Create Spark Context
    sc = SparkContext("local", "HTRC Aggregator")
    sc.setLogLevel("ERROR")

    # Open the list of all htrc volumes
    vols = open('htrc-ef-all-files.txt')

    # Download temp files "iters" number of times
    for i in iters:
        # Set flag to limit number of volumes to download
        j = 0

        # Download files as tmp0-tmpX, overwriting previous tmp files
        for vol in vols:
            #print('Downloading volume...')
            os.system("rsync -av --no-relative data.analytics.hathitrust.org::features/" + 
                      vol.rstrip() + " ./tmp" + str(j) + " >/dev/null 2>&1")
            j += 1
            if j == tmps:
                j = 0
                break

        # Get the paths to all the tmp files
        tmpfiles = glob.glob('./tmp*')
        print(tmpfiles)

        # Load the files into the feature reader
        htvols = FeatureReader(tmpfiles)

        # Create a Spark datastructure out of the FeatureReader volumes
        scvols = sc.parallelize(htvols)

        # Extract the relevant Part of Speech tagged data from the volumes
        db = scvols.flatMap(lambda x: extract_database(x, pos, lang))

        if i == 0:
            # In the first iteration, just aggregate across the volumes
            rdb = db.reduceByKey(lambda a, b: a+b)
        else:
            # Later, combine with previous aggregations
            # and then aggregate again
            rdb = db.union(rdb).reduceByKey(lambda a, b: a+b)

        # Save results
        rdb.saveAsTextFile('output'+str(i)+'-'+str(tmps))

        # Delete tmp files
        for f in tmpfiles:
            os.remove(f)
