import glob
from subprocess import check_output
import os
import sys
import pandas as pd
import pyrsync2 as rsync
from itertools import islice
from htrc_features import FeatureReader
from htrc_features import utils as htrcutils
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import StorageLevel

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
                # Extract data that matches the part of speech tag
                tl = vol.tokenlist(pages=False,
                                    case=False,
                                    pos=True).filter(like=pos,axis=0)
                tl.reset_index(inplace=True)

                # Filter out non-alphanumberic, numeric tokens, and smaller
                # than three characters
                mask = (tl['lowercase'].str.isalnum()) & \
                        ~(tl['lowercase'].str.isdigit()) & \
                         (tl['lowercase'].str.len() >= 3)
                tl = tl.loc[mask]

                return [((x[2],vol.year,vol.pub_place,vol.genre[0]),x[-1])
                        for x in tl.to_records()]
            except:
                return []
        else:
            try:
                # Extract all of the data
                tl = vol.tokenlist(pages=False,
                                    case=False,
                                    pos=True)
                # Filter out non-alphanumberic, numeric tokens, and smaller
                # than three characters
                mask = (tl['lowercase'].str.isalnum()) & \
                        ~(tl['lowercase'].str.isdigit()) & \
                         (tl['lowercase'].str.len() >= 3)
                tl = tl.loc[mask]

                return [((x[2],vol.year,vol.pub_place,vol.genre[0]),x[-1])
                        for x in tl.to_records()]
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
    startvol = 0
    autocont = False
    spark = ''

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
            elif s[0] == 'start':
                startvol = int(s[1])
            elif s[0] == 'autocont':
                autocont = True
            elif s[0] == 'spark':
                spark = s[1]
        except:
            continue

    if debug:
        # Print out some of the flag values
        print('POS: '+pos+'\tIters: '+str(iters)+'\tTmps: '+
              str(tmps)+'\tLang: '+lang+'\n')

    # Create Spark Context
    sconf = SparkConf().setMaster(spark)
    sconf.setAppName('HTRC Aggregator')
    sconf.set("spark.executor.memory", "6g")
    sconf.set("spark.cores.max", "6")
    sc = SparkContext(conf=sconf)
    sc.setLogLevel("ERROR")

    # Open the list of all htrc volumes
    vols = open('htrc-ef-all-files.txt')

    # Jump to current volume
    if startvol != 0:
        volsamples = list(islice(vols, startvol))

    # Create a counter for volumes read and save as total number of volumes
    totvols = int(check_output(["wc", "-l", 'htrc-ef-all-files.txt']))
    numvols = int(totvols)

    # Keep running code while there are unread volumes
    while totvols > 0:
        # Get the initial value for this round
        mystart = numvols - totvols

        # Download temp files "iters" number of times
        for i in iters:
            print('Iteration: ' + str(i+1))

            # Create a file with the next set of volumes
            volsamples = list(islice(vols, tmps))
            totvols -= tmps
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

            db.unpersist()
            del db

            # Delete tmp files
            for f in tmpfiles:
                os.remove(f)

        # Get the final volume for this round
        myend = numvols - totvols

        #
        outfile = open('output-'+str(mystart)+'-'+str(myend)+'.txt','w')
        outfile.write('word\tyear\tplace\tgenre\tcount\n')

        for item in outdb.collect():
            outfile.write('\t'.join(item[0])+'\t'+str(item[1])+'\n')

        outfile.close()

        outdb.unpersist()
        del outdb

        if not autocont:
            answer = input('Completed another round. Continue parsing?\t')
            if answer == 'n':
                break
