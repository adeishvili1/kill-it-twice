SEED_ROWS ?= 1000000
PIPELINE_URL ?= http://localhost:3000

.PHONY: up down seed verify logs test build ps clean

up:            ## build and start the whole system
	docker compose up -d --build
	@echo "UI: http://localhost:4200   pipeline API: $(PIPELINE_URL)   RabbitMQ: http://localhost:15672 (guest/guest)"

build:
	docker compose build

down:          ## stop everything, keep data
	docker compose down

clean:         ## stop everything and delete volumes (schema re-applies on next up)
	docker compose down -v

ps:
	docker compose ps

seed:          ## reset sinks + checkpoints and generate SEED_ROWS source rows
	@./verify/wait-for.sh $(PIPELINE_URL)/health 120
	@echo "seeding $(SEED_ROWS) rows ..."
	@curl -sf --max-time 600 -X POST "$(PIPELINE_URL)/api/admin/seed" \
	  -H 'content-type: application/json' -d '{"rows": $(SEED_ROWS)}' | jq .

verify:        ## run the five gates and print the report
	SEED_ROWS=$(SEED_ROWS) ./verify.sh

logs:
	docker compose logs -f --tail=200 pipeline consumer

test:          ## unit tests
	cd apps/pipeline && npm test
	cd apps/consumer && npm test
