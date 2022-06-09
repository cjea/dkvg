.PHONY: phony

build: phony ## Build main.go into ./dkvg
	go build -o dkvg main.go

run: phony ## Run main.go
	go run main.go

BLUE := $(shell tput setaf 4)
RESET := $(shell tput sgr0)
.PHONY: help
help: ## List all targets and short descriptions of each
	@grep -E '^[^ .]+: .*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk '\
			BEGIN { FS = ": .*##" };\
			{ printf "$(BLUE)%-29s$(RESET) %s\n", $$1, $$2  }'
