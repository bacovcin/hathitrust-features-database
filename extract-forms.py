infile = open('output.txt')
infile.readline()
forms = {}
for line in infile:
    forms[line.split('\t')[0]] = ''

outfile = open('forms.txt','w')
for key in forms:
    outfile.write(key+'.\n')
