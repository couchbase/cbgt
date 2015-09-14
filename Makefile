default: build

build: gen-bindata
	go build ./...
	go build ./cmd/cbgt-rebalance
	go build ./cmd/cbgt-planner

gen-bindata:
	(cd rest; \
     go-bindata-assetfs -pkg=rest ./static/...; \
     go fmt bindata_assetfs.go)

coverage:
	go test -coverprofile=coverage.out -covermode=count
	go test -coverprofile=coverage-cmd.out -covermode=count ./cmd
	go test -coverprofile=coverage-rest.out -covermode=count ./rest
	go test -coverprofile=coverage-rest-monitor.out -covermode=count ./rest/monitor
	go test -coverprofile=coverage-rebalance.out -covermode=count ./rebalance
	cat coverage-cmd.out | grep -v "mode: count" >> coverage.out
	cat coverage-rest.out | grep -v "mode: count" >> coverage.out
	cat coverage-rest-monitor.out | grep -v "mode: count" >> coverage.out
	cat coverage-rebalance.out | grep -v "mode: count" >> coverage.out
	go tool cover -html=coverage.out

