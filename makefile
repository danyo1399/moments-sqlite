install:
	go install gotest.tools/gotestsum@latest
test:
	gotestsum --format testname ./...
prepare:
	go mod tidy
	go test ./...
testwatch:
	gotestsum --watch --format testname --packages="./..."
