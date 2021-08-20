.PHONY: proto
proto:
	protoc -I. --go_out=plugins=grpc,paths=source_relative:. ./internal/proto/testpb/*.proto

.PHONY: generate
generate: proto
	go generate ./...

.PHONY: cover
cover: generate
	go test -coverprofile=coverage.out .
	go tool cover -func=coverage.out

.PHONY: example
example: cover
	env GOOS=linux GOARCH=amd64 go build -o ./bin/example_linux ./example
	env GOOS=windows GOARCH=amd64 go build -o ./bin/example_windows.exe ./example