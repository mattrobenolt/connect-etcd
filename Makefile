gomod := go.withmatt.com/connect-etcd

PROTO_OUT := types
PROTO_SRC := proto-src

BIN := bin

clean: clean-proto clean-bin

clean-proto:
	rm -rf $(PROTO_OUT)

clean-bin:
	rm -rf $(BIN)

$(BIN):
	mkdir -p $(BIN)

$(PROTO_OUT):
	mkdir -p $(PROTO_OUT)

TOOL_INSTALL := env GOBIN=$(PWD)/$(BIN) go install

$(BIN)/protoc-gen-go: Makefile | $(BIN)
	$(TOOL_INSTALL) google.golang.org/protobuf/cmd/protoc-gen-go

$(BIN)/protoc-gen-go-vtproto: Makefile | $(BIN)
	$(TOOL_INSTALL) github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.4.0

$(BIN)/protoc-gen-connect-go: Makefile | $(BIN)
	$(TOOL_INSTALL) github.com/bufbuild/connect-go/cmd/protoc-gen-connect-go

$(BIN)/gofumpt: Makefile | $(BIN)
	$(TOOL_INSTALL) mvdan.cc/gofumpt@v0.3.1

$(BIN)/staticcheck: Makefile | $(BIN)
	$(TOOL_INSTALL) honnef.co/go/tools/cmd/staticcheck@v0.3.3

$(BIN)/enumcheck: Makefile | $(BIN)
	$(TOOL_INSTALL) loov.dev/enumcheck@8aa7b787306eb6f75b5cfac842ead25134f459ce

$(BIN)/govulncheck: Makefile | $(BIN)
	$(TOOL_INSTALL) golang.org/x/vuln/cmd/govulncheck@latest

$(BIN)/buf: Makefile | $(BIN)
	$(TOOL_INSTALL) github.com/bufbuild/buf/cmd/buf@v1.15.1

$(BIN)/yq: Makefile | $(BIN)
	$(TOOL_INSTALL) github.com/mikefarah/yq/v4@v4.27.3

PROTO_TOOLS := $(BIN)/protoc-gen-go $(BIN)/protoc-gen-connect-go $(BIN)/protoc-gen-go-vtproto $(BIN)/buf
tools: $(PROTO_TOOLS) $(BIN)/gofumpt $(BIN)/staticcheck $(BIN)/enumcheck $(BIN)/govulncheck $(BIN)/yq

proto: $(PROTO_TOOLS) | $(PROTO_OUT)
	$(BIN)/buf generate

fmt: fmt-go fmt-proto

fmt-go: $(BIN)/gofumpt
	$(BIN)/gofumpt -l -w .

fmt-proto: $(BIN)/buf
	$(BIN)/buf format -w

fmt-yaml: $(BIN)/yq
ifeq (, $(shell command -v fd 2>/dev/null))
	@echo "!! Maybe install 'fd', it's a lot faster (https://github.com/sharkdp/fd)"
	find . -type f \( -name '*.yaml' -o -name '*.yml' \) -exec $(BIN)/yq -iP eval-all . {} \;
else
	fd . -t f -e yaml -e yml -x $(BIN)/yq -iP eval-all . {} \;
endif

lint: lint-vet lint-staticcheck lint-enumcheck lint-govulncheck lint-proto

lint-vet:
	go vet ./...

lint-staticcheck: $(BIN)/staticcheck
	$(BIN)/staticcheck -f=stylish ./...

lint-enumcheck: $(BIN)/enumcheck
	$(BIN)/enumcheck ./...

lint-govulncheck: $(BIN)/govulncheck
	$(BIN)/govulncheck ./...

lint-proto: $(BIN)/buf
	$(BIN)/buf lint -v

tests:
	go test -v ./...

update:
	go get -v -u ./...
	go mod tidy
	$(MAKE) clean proto

.PHONY: proto tools update \
		clean clean-proto clean-bin \
		fmt fmt-go fmt-proto fmt-yaml \
		lint lint-vet lint-staticcheck lint-enumcheck lint-govulncheck lint-proto \
		tests
