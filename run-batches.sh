mkdir ./filelists/;
mkdir ./hathitrust-textfiles;
mkdir ./hathitrust-rawfiles;
mkdir ./hathitrust-textfiles/metadata;
mkdir ./hathitrust-textfiles/tokens;
echo Downloading Hathitrust files;
rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt .;
for i in $(seq 1 20341)
do
	python create-downlist.py ./moved-filelists/list-$1.txt
done
rsync -av --info=progress2 --no-relative --files-from downlist.txt data.analytics.hathitrust.org::features/ ./hathitrust-rawfiles/
for i in $(seq 2000 1)
do
	bash ~/Github/hathitrust-features-database/run-batches-internal.sh $i;
done
