.PHONY: phony

build: phony ## Build main.go into ./dkvg
	go build -o dkvg main.go

SOCKET ?= /tmp/dkvg.sock
run: free-socket phony ## Run main.go
	go run main.go --sock $(SOCKET)

free-socket: phony ## Remove SOCKET if in use.
	rm $(SOCKET) 2>/dev/null || true

client: phony ## Connect to the Unix domain socket of a dkvg server.
	nc -U $(SOCKET)

BLUE := $(shell tput setaf 4)
RESET := $(shell tput sgr0)
.PHONY: help
help: ## List all targets and short descriptions of each
	@grep -E '^[^ .]+: .*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk '\
			BEGIN { FS = ": .*##" };\
			{ printf "$(BLUE)%-29s$(RESET) %s\n", $$1, $$2  }'
