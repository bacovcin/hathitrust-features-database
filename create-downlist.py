import sys

print('Loading original filelists')
filelist = set()
for arg in sys.argv:
    if arg[-3:] == 'txt':
        infile = open(sys.argv[-1])
        with open('filelists/' + arg.split('/')[-1], 'w') as outfile:
            for line in infile:
                sline = line.rstrip().split('/')
                filelist.add(sline[-1])
                outfile.write('hathitrust-rawfiles/' + sline[-1] + '\n')

print('Creating downlist...')
hathitrustlist = open('htrc-ef-all-files.txt')
outfile = open('downlist.txt', 'a')
for line in hathitrustlist:
    sline = line.rstrip().split('/')
    if sline[-1] in filelist:
        outfile.write(line)
