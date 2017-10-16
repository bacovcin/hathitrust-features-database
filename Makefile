batches := $(addprefix /big/hathitrust-textfiles/lemmata/,$(addsuffix .txt,$(basename $(notdir $(wildcard filelists/*.txt)))))


$(ycoetargs) : corpora-final/YCOE/%.psd : lemmatisation-scripts/recombine-lemmalist-OE.py corpora-out/YCOE/%.txt
	@echo --- Recombining $@ ---
	@mkdir -p $(@D)
	@python $< $(addprefix corpora/YCOE/,$(notdir $@))

$(batches) : /big/hathitrust-textfiles/lemmata/%.txt : filelists/%.txt
	@echo --- Parsing $@ ---
	sbt -J-d64 -J-Xmx16G -J-XX:-UseGCOverheadLimit "run $(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))) 1000 4 6"
	@echo --- Done with $< ---

all : $(batches)
