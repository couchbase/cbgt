coverage:
	go test -coverprofile=coverage.out -covermode=count
	go test -coverprofile=coverage-cmd.out -covermode=count ./cmd
	go test -coverprofile=coverage-rest.out -covermode=count ./rest
	cat coverage-cmd.out | grep -v "mode: count" >> coverage.out
	cat coverage-rest.out | grep -v "mode: count" >> coverage.out
	go tool cover -html=coverage.out

