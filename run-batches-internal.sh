echo --- Parsing list-$1.txt ---;
if [ -f ./new-filelists/list-$1.txt ]; then
	if [ $1 -lt 200 ]; then
		sbt -J-d64 -J-Xmx24G -J-XX:-UseGCOverheadLimit "run $1 20 4 8 300 100000";
	else
		if [ $1 -lt 4000 ]; then
			sbt -J-d64 -J-Xmx24G -J-XX:-UseGCOverheadLimit "run $1 20 2 7 400 50000";
		else
			sbt -J-d64 -J-Xmx24G -J-XX:-UseGCOverheadLimit "run $1 20 2 5 500 10000";
		fi
	fi
	echo --- Done with list $1 ---
	gzip -v --fast /big/hathitrust-textfiles/metadata/$1-new.txt &
	gzip -v --fast /big/hathitrust-textfiles/tokens/$1-new.txt &
	gzip -v --fast /big/hathitrust-textfiles/lemmata/$1-new.txt &
	mv ./new-filelists/list-$1.txt finished-filelists/list-new-$1.txt
fi
