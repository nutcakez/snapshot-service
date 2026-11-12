.DEFAULT_GOAL := help
.PHONY: help run test test-race docker-run docker-test

help: ## Show the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

run: ## Run the service locally on :8080 (needs Go)
	go run ./cmd/server

test: ## Run all tests locally
	go test -count=1 ./...

test-race: ## Run all tests locally with the race detector
	go test -race -count=1 ./...

docker-run: ## Build and run the service in Docker on :8080
	docker compose up --build

docker-test: ## Build and run all tests with the race detector in Docker
	docker compose run --rm --build test
