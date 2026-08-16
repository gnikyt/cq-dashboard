PKG=github.com/gnikyt/cq-dashboard

.PHONY: all test test-coverage test-race demo fmt vet lint tidy clean

all: fmt vet test

test:
	go test -timeout 60s ./...

test-coverage:
	go test -timeout 60s -coverprofile=/tmp/cq-dashboard-cover ./...

test-race:
	go test -timeout 120s -race ./...

demo:
	go run ./cmd/demo

fmt:
	gofmt -l -w .

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	go clean
	rm -f /tmp/cq-dashboard-cover cq-dashboard.db
