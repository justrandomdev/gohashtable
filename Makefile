BINARY_NAME=hashtest
REPO_NAME=hashtest
BUILD_TIME=$(shell date | sed 's| |_|'g)

build:
	go build \
		-mod=vendor \
		-ldflags="-s -w -X main.buildTime=$(BUILD_TIME)" \
		./...

linux:
	CGO_ENABLED=0 \
	GOOS=linux \
	GO111MODULE=auto \
	go build \
		-mod=vendor \
		-ldflags="-s -w -X main.buildTime=$(BUILD_TIME)" \
		-gcflags=all=-trimpath="${CURDIR}" \
		-asmflags=all=-trimpath="${CURDIR}" \
		-o $(BINARY_NAME) ./cmd/$(BINARY_NAME)

windows: 
	CGO_ENABLED=0 \
	GOOS=windows \
	GO111MODULE=auto \
	go build \
		-mod=vendor \
		-ldflags="-s -w -X main.buildTime=$(BUILD_TIME)" \
		-gcflags=all=-trimpath="${CURDIR}" \
		-asmflags=all=-trimpath="${CURDIR}" \
		-o $(BINARY_NAME).exe ./cmd/$(BINARY_NAME)


vendor:
	go mod tidy
	go mod vendor

gofmt:
	gofmt -l -s -w ./pkg ./cmd

clean:
	rm -f $(BINARY_NAME)
