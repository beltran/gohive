package gohive

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"testing"
)

func TestSaslTransport(t *testing.T) {
	configuration := map[string]string{
		"username": "jmarhuenda",
	}
	trans, err := NewTSaslTransport(thrift.NewTMemoryBuffer(), "localhost", "PLAIN", configuration)
	if err != nil {
		t.Fatal("Error creating transport")
	}
	trans.IsOpen()
}
