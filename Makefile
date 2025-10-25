DATA?=$(PWD)/data

.PHONY: build prepare prepare-wider run run-adaptive run-original baseline clean

build:
	docker build -t applovin-engine .

prepare:
	docker run --rm -v $(DATA):/data --entrypoint python applovin-engine \
	  /app/prepare.py --raw /data/raw --lake /data/lake --mvs /data/mvs --threads 4 --mem 8GB

prepare-wider:
	docker run --rm -v $(DATA):/data --entrypoint python applovin-engine \
	  /app/prepare_wider_mvs_only.py --lake /data/lake --mvs /data/mvs --threads 4 --mem 6GB

run: run-adaptive

run-adaptive:
	docker run --rm -v $(DATA):/data -v $(PWD)/queries:/queries -v $(DATA)/outputs:/outputs applovin-engine \
	  --lake /data/lake --mvs /data/mvs --queries /queries --out /outputs --threads 4 --mem 4GB --analyze

run-original:
	docker run --rm -v $(DATA):/data -v $(PWD)/queries:/queries -v $(DATA)/outputs:/outputs \
	  --entrypoint python applovin-engine /app/runner.py \
	  --lake /data/lake --mvs /data/mvs --queries /queries --out /outputs --threads 4 --mem 4GB

baseline:
	docker run --rm -v $(DATA):/data -v $(PWD)/queries:/queries -v $(DATA)/outputs:/outputs \
	  --entrypoint python applovin-engine /app/baseline_main.py \
	  --data-dir /data/lake --out-dir /outputs/baseline --queries /queries --mode lake

clean:
	rm -rf $(DATA)/lake $(DATA)/mvs $(DATA)/outputs
