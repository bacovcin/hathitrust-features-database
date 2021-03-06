batches := $(addprefix /big/hathitrust-textfiles/lemmata/,$(addsuffix -new.txt.gz ,$(subst list-, , $(basename $(notdir $(wildcard filelists/*.txt) ) ) ) ) )

$(batches) : /big/hathitrust-textfiles/lemmata/%-new.txt.gz : filelists/list-%.txt
	@echo --- Parsing $@ ---
	sbt -J-d64 -J-Xmx24G -J-XX:-UseGCOverheadLimit "run $(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))) 3 3 9 300"
	@echo --- Done with $< ---
	@gzip -v --fast /big/hathitrust-textfiles/metadata/$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2))))-new.txt
	@gzip -v --fast /big/hathitrust-textfiles/tokens/$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2))))-new.txt
	@gzip -v --fast /big/hathitrust-textfiles/lemmata/$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2))))-new.txt
	@mv $< finished-filelists/list-new-$(firstword $(subst ., ,$(or $(word 2,$(subst -, ,$<)),$(value 2)))).txt

all : $(batches)
