#!/bin/bash
thrift -r --gen go thrift/hive_metastore.thrift
cp -r gen-go/hive_metastore .
cp -r gen-go/fb303 .

