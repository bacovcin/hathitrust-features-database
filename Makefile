output.txt : htrc-ef-all-files.txt aggregate.py
	@echo --- Extracting features ---
	python aggregate.py pos=VBD iters=5 tmps=2000 spark=spark://hbacovci-linux-desktop:7077

forms.txt : all.txt extract-forms.py
	@echo --- Extracting forms ---
	python extract-forms.py

lemmata.txt : forms.txt eccoplaintext.properties morphadorner/data/eccolexicon.lex morphadorner/data/ncftransmat.mat morphadorner/data/eccosuffixlexicon.lex morphadorner/data/ncfmergedspellingpairs.tab morphadorner/data/eccomergedspellingpairs.tab morphadorner/data/standardspellings.txt morphadorner/data/spellingsbywordclass.txt
	@echo --- Lemmatising forms ---
	cd morphadorner && \
	java -Xmx8g -Xss1m -cp :bin/:dist/*:lib/* \
		edu.northwestern.at.morphadorner.MorphAdorner \
		-p ../eccoplaintext.properties \
		-l data/eccolexicon.lex \
		-t data/ncftransmat.mat \
		-u data/eccosuffixlexicon.lex \
		-a data/ncfmergedspellingpairs.tab \
		-a data/eccomergedspellingpairs.tab \
		-s data/standardspellings.txt \
		-w data/spellingsbywordclass.txt \
		-o ../lemmata \
		../forms.txt && \
	cd ../ && \
	mv ./lemmata/forms.txt lemmata.txt && \
	rm -r ./lemmata

lemma-output.txt : all.txt lemmata.txt incorporate-lemmata.py
	@echo --- Incorporating lemmata ---
	python incorporate-lemmata.py
