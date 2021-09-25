#!/bin/bash
protoc --go_out=plugins=grpc:. cmd/proto/rosedb.proto
protoc --go_out=plugins=grpc:. cmd/proto/pb_set.proto
protoc --go_out=plugins=grpc:. cmd/proto/pb_hash.proto
protoc --go_out=plugins=grpc:. cmd/proto/pb_zset.proto
protoc --go_out=plugins=grpc:. cmd/proto/pb_str.proto
protoc --go_out=plugins=grpc:. cmd/proto/pb_list.proto