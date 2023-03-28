.PHONY: build
build:
	go build -v ./cmd/kafka-canary

.PHONY: test
test:
	go test -v -cover -race ./...

.PHONY: fmt
fmt:
	gofmt -l -s -w ./
	goimports -l --local "github.com/pecigonzalo/kafka-canary" -w ./

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	generate ./...
