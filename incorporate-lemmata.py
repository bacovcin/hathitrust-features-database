infile = open('lemmata.txt')
lemmata = {}
for line in infile:
    s = line.rstrip().split('\t')
    if s[2] in ['vvn','vvd']:
        lemmata[s[1]] = (s[3],s[4])
infile.close()

infile = open('all.txt')
infile.readline()
new_output = {}
for line in infile:
    s = line.rstrip().split('\t')
    if s[0] in lemmata.keys():
        lemma = lemmata[s[0]]
        key = (lemma[1],lemma[0],s[1],s[2],s[3])
        count = int(s[-1])
        try:
            new_output[key] += count
        except:
            new_output[key] = count

outfile = open('lemma-output.txt','w')
outfile.write('lemma\tnormform\tyear\tplace\tgenre\tcount\n')
for key in new_output:
    outfile.write('\t'.join(key)+'\t'+str(new_output[key])+'\n')
