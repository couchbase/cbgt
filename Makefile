default: build

build: gen-bindata
	go build ./...
	go build ./cmd/cbgt-rebalance

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

# To release a new version of the cbgt library...
#
#   git grep v0.2.0 # Look for old version strings.
#   git grep v0.2   # Look for old version strings.
#   # Edit/update files, if any.
#   # Ensure bindata_assetfs.go is up-to-date, by running...
#   make build
#   # Then, run tests...
#   go test ./...
#   # Then, run a diff against the previous version...
#   git log --pretty=format:%s v0.2.0...
#   # Then, update the CHANGES.md file based on diff.
#   git commit -m "v0.3.0"
#   git push
#   git tag -a "v0.3.0" -m "v0.3.0"
#   git push --tags
