.PHONY: build test test-ui check demo

build:
	mkdir -p bin
	go build -o bin/vtransfer ./cmd/vtransfer

test:
	go test -race ./...

test-ui:
	node --test tests/*.test.mjs

check:
	go vet ./...
	@for file in control/web/*.js; do node --check "$$file" || exit 1; done

demo: build
	./bin/vtransfer serve -demo
