package gohive

import (
	"testing"
	"git.apache.org/thrift.git/lib/go/thrift"
)

func TestSaslTransport(t *testing.T) {
	configuration := map[string]string{
		"username": "jmarhuenda",
	}
	trans, err := NewTSaslTransport(thrift.NewTMemoryBuffer(), "localhost", "PLAIN", configuration)
	if err != nil {
		t.Fatal("Erro creating transport")
	}
	trans.IsOpen()
}
