#!/bin/bash
dep ensure -update
thrift -r --gen go thrift/HiveServer.thrift
cp -r gen-go/hiveserver vendor/
