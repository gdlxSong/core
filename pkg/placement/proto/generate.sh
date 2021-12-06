# generate pb.



protoc --go_out=./v1 ./v1/*.proto
protoc --go-grpc_out=./v1 ./v1/*.proto