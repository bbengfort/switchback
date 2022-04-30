package api

//go:generate protoc -I=$GOPATH/src/github.com/bbengfort/switchback/proto --go_out=. --go_opt=module=github.com/bbengfort/switchback/pkg/api/v1 --go-grpc_out=. --go-grpc_opt=module=github.com/bbengfort/switchback/pkg/api/v1 switchback/v1/switchback.proto
