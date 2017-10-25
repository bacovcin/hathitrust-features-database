batches := $(addprefix /big/hathitrust-textfiles/lemmata/,$(addsuffix .txt.gz ,$(subst list-, , $(basename $(notdir $(wildcard filelists/*.txt) ) ) ) ) )

$(batches) : /big/hathitrust-textfiles/lemmata/%.txt.gz : filelists/list-%.txt
	@echo --- Parsing $@ ---
	sbt -J-d64 -J-Xmx24G -J-XX:-UseGCOverheadLimit "run $(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))) 500 4 6"
	@echo --- Done with $< ---
	@gzip -v --fast /big/hathitrust-textfiles/metadata/$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))).txt
	@gzip -v --fast /big/hathitrust-textfiles/tokens/$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))).txt
	@gzip -v --fast /big/hathitrust-textfiles/lemmata/$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))).txt
	@mv $< finished-filelists/list-$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))).txt

all : $(batches)
