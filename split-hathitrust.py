import os

# Download up to date filelist
os.system("rsync -azv " +
          "data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt" +
          " .")

# Load the file list
infile = open("htrc-ef-all-files.txt")
i = 0
j = 1

# Write out each set of 10000 files as their own list
curout = open("filelists/list-"+str(j)+'.txt', 'w')
print(j)
for line in infile:
    i += 1
    if i % 10000 == 0:
        print(j)
        j += 1
        curout = open("filelists/list-"+str(j)+'.txt', 'w')
    curout.write(line)
