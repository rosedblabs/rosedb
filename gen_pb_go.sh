#!/bin/bash
protoc --go_out=plugins=grpc:. cmd/proto/rosedb.proto
protoc --go_out=plugins=grpc:. cmd/proto/pb_set.proto