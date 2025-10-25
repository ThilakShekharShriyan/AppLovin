DATA?=$(PWD)/data

.PHONY: build prepare run baseline clean

build:
	docker build -t applovin-engine .

prepare:
	docker run --rm -v $(DATA):/data --entrypoint python applovin-engine \
	  /app/prepare.py --raw /data/raw --lake /data/lake --mvs /data/mvs

run:
	docker run --rm -v $(DATA):/data -v $(PWD)/queries:/queries -v $(DATA)/outputs:/outputs applovin-engine \
	  --lake /data/lake --mvs /data/mvs --queries /queries --out /outputs --threads 8 --mem 6GB

baseline:
	docker run --rm -v $(DATA):/data -v $(PWD)/queries:/queries -v $(DATA)/outputs:/outputs \
	  --entrypoint python applovin-engine /app/baseline_main.py \
	  --data-dir /data/raw --out-dir /outputs --queries /queries --mode csv

clean:
	rm -rf $(DATA)/lake $(DATA)/mvs $(DATA)/outputs
