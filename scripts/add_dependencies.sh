#!/bin/bash
thrift -r --gen go thrift/HiveServer.thrift
cp -r gen-go/hiveserver .

# For some reason these files have to be deleted. Getting duplicate definition
rm hiveserver/tcliservice.go hiveserver/ttypes.go hiveserver/constants.go
