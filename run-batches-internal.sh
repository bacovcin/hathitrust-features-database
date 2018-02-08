echo --- Parsing list-$1.txt ---;
if [ -f ./filelists/list-$1.txt ]; then
	echo Processing files...;
	sbt -J-d64 -J-Xmx24G -J-XX:-UseGCOverheadLimit "run $1 4 6 300";
	echo --- Done with list $1 ---
	gzip -v --fast ./hathitrust-textfiles/metadata/$1-new.txt &
	gzip -v --fast ./hathitrust-textfiles/tokens/$1-new.txt &
	mv ./filelists/list-$1.txt finished-filelists/list-new-$1.txt
fi
