package datatypes

//go:generate protoc -I $GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.1 -I . --plugin ../../../scripts/protoc-gen-gogofaster --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage_common.proto predicate.proto
