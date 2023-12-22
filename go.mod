module github.com/beltran/gohive

go 1.19

require (
	github.com/apache/thrift v0.18.1
	github.com/beltran/gosasl v0.0.0
	github.com/go-zookeeper/zk v1.0.1
	github.com/pkg/errors v0.9.1
)

replace github.com/beltran/gosasl v0.0.0 => "/root/gosasl"

require github.com/beltran/gssapi v0.0.0-20200324152954-d86554db4bab // indirect
