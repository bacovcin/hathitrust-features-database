import glob
from subprocess import check_output
import os
import sys
import pandas as pd
import pyrsync2 as rsync
from itertools import islice
from operator import add
import ujson
import bz2
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import StorageLevel

def extract_database(word, year, pp, gen, pos):
    '''Function for mapping htrc_features volumes to a list of tuples with
    the following structure ((word, pos, year, place, genre), count)'''
    output = []

    form = word[0]
    wdict = word[1]
    if pos == '':
        return [((form,year,pp,gen), sum(wdict.values()))]
    else:
        try:
            return [((form,year,pp,gen), wdict[pos])]
        except:
            return []

if __name__ == "__main__":
    try:
        # Remove environment variables that can cause errors
        del os.environ['PYSPARK_SUBMIT_ARGS']
    except:
        pass

    # Set defaults for flags
    pos = ''
    lang = 'eng'
    iters = 5
    tmps = 20
    downsize = 1000
    debug=False
    spark = ''

    # Set flags if passed as arguments
    for x in sys.argv:
        try:
            s = x.split('=')
            if s[0] == 'pos':
                pos = s[1]
            elif s[0] == 'iters':
                iters = int(s[1])
            elif s[0] == 'downsize':
                downsize = int(s[1])
            elif s[0] == 'tmps':
                tmps = int(s[1])
            elif s[0] == 'lang':
                lang = s[1]
            elif s[0] == 'debug':
                debug = True
            elif s[0] == 'spark':
                spark = s[1]
        except:
            continue

    if debug:
        # Print out some of the flag values
        print('POS: '+pos+'\tIters: '+str(iters)+'\tTmps: '+
              str(tmps)+'\tLang: '+lang+'\n')

    # Open the list of all htrc volumes
    vols = open('htrc-ef-all-files.txt')

    # Run "iters" number of iterations
    i = 0
    while i < iters:
        print('Iteration: '+str(i+1))
        if debug:
            print('Iteration: ' + str(i+1))

        # Create Spark Context
        sconf = SparkConf().setMaster(spark)
        sconf.setAppName('HTRC Aggregator')
        sconf.set("spark.executor.memory", "2g")
        sconf.set("spark.cores.max", "7")
        sc = SparkContext(conf=sconf)
        sc.setLogLevel("ERROR")
        #sc = SparkContext("local", "HTRC Aggregator")

        # Create a file with the next set of volumes
        volsamples = list(islice(vols, downsize))
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
        oldfiles = list(tmpfiles)

        while len(tmpfiles) > 0:
            voldbs = []
            # Create a separate RDD for each volume
            for j in range(tmps):
                try:
                    volname = tmpfiles.pop(0)
                except IndexError:
                    continue
                volfile = ujson.loads(bz2.open(volname).readline())
                if volfile['metadata']['language'] == lang or lang == '':
                    year = volfile['metadata']['pubDate']
                    pp = volfile['metadata']['pubPlace']
                    gen = '+'.join(volfile['metadata']['genre'])
                    dL = [x['body']['tokenPosCount']
                          for x in volfile['features']['pages']]
                    vol = sc.parallelize([(k.lower(), v)
                                          for d in dL
                                          for k, v in d.items()
                                          if (len(k) >= 3 and
                                             k.isalnum() and
                                             not k.isdigit())])
                    voldbs.append(vol.flatMap(lambda x: extract_database(x,
                                                                         year,
                                                                         pp,
                                                                         lang,
                                                                         pos)))

            # Combine all the volume RDDs into one large RDD
            db = sc.union(voldbs)

            # Group identical keys (including from previous batches)
            try:
                olddb = sc.parallelize(outs)
                outdb = olddb.union(db).foldByKey(0,add)
            except:
                outdb = db.foldByKey(0, add)

            # Store the current batch's output
            outs = outdb.collect()

        # Delete tmp files
        for f in oldfiles:
            os.remove(f)

        # End the current Spark job
        sc.stop()

        # Increase batch number
        i += 1

    # Open the output Tab-separated Value file and write out header
    outfile = open('output.txt','w')
    outfile.write('word\tyear\tplace\tgenre\tcount\n')

    # Write all of the key-value pairs into the file
    for item in outs:
        outfile.write('\t'.join(item[0])+'\t'+str(item[1])+'\n')

    outfile.close()
