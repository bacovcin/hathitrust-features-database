import glob
import os
import sys
import pandas as pd
import pyrsync2 as rsync
import pickle
from itertools import islice
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
            try:
                return [((x[1],vol.year),x[3])
                        for x in vol.tokenlist(pages=False,
                                               case=False,
                                               pos=True).filter(like=pos,axis=0).to_records()]
            except:
                return []
        else:
            try:
                return [((x[1],vol.year),x[3])
                        for x in vol.tokenlist(pages=False,
                                               case=False,
                                               pos=True).to_records()]
            except:
                return []
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
    debug=False

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
            elif s[0] == 'debug':
                debug = True
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
        print('Iteration: ' + str(i+1))

        # Create a file with the next set of volumes
        volsamples = list(islice(vols, tmps))
        if debug:
            print(volsamples)
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

        # Get the paths to all the tmp files
        tmpfiles = glob.glob('*.bz2')
        if debug:
            print(tmpfiles)

        # Load the files into the feature reader
        htvols = FeatureReader(tmpfiles)

        # Create a Spark datastructure out of the FeatureReader volumes
        scvols = sc.parallelize(htvols)

        # Extract the relevant Part of Speech tagged data from the volumes
        db = scvols.flatMap(lambda x: extract_database(x, pos, lang))


        if i == 0:
            outdb = db.reduceByKey(lambda a, b: a+b)
        else:
            outdb = outdb.union(db).reduceByKey(lambda a, b: a+b)

        # Delete tmp files
        for f in tmpfiles:
            os.remove(f)

    outfile = open('output.txt','w')
    outfile.write('word\tyear\tcount\n')

    for item in outdb.collect():
        outfile.write('\t'.join(item[0])+'\t'+str(item[1])+'\n')

    outfile.close()
