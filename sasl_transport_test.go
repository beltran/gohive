package gohive

import (
	"context"
	"io"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
)

func TestSaslTransport(t *testing.T) {
	configuration := map[string]string{
		"username": "jmarhuenda",
	}
	trans, err := NewTSaslTransport(thrift.NewTMemoryBuffer(), "localhost", "PLAIN", configuration)
	if err != nil {
		t.Fatal("Error creating transport")
	}
	if trans.IsOpen() {
		t.Fatal("Transport shouldn't be opened yet")
	}
	trans.Open()
	if !trans.IsOpen() {
		t.Fatal("Transport shouldbe opened")
	}
}

func setup() {
	transport_bdata = make([]byte, TRANSPORT_BINARY_DATA_SIZE)
	for i := 0; i < TRANSPORT_BINARY_DATA_SIZE; i++ {
		transport_bdata[i] = byte((i + 'a') % 255)
	}
	transport_header = map[string]string{"key": "User-Agent",
		"value": "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36"}
}

func TestSaslTransportThrift(t *testing.T) {
	configuration := map[string]string{
		"username": "thriftUsername",
		"password": "thriftPassword",
	}
	setup()

	socket := thrift.NewTMemoryBuffer()
	trans, err := NewTSaslTransport(socket, "localhost", "PLAIN", configuration)
	if err != nil {
		t.Fatal(err)
	}
	trans.Open()
	trans.SetMaxLength(1638400)
	socket.Reset()
	TransportTest(t, trans, trans)
	if trans.RemainingBytes() != 0 {
		t.Fatal("Expected 0 remaining bytes but found: ", trans.RemainingBytes())
	}
}

const TRANSPORT_BINARY_DATA_SIZE = 4096

var (
	transport_bdata  []byte // test data for writing; same as data
	transport_header map[string]string
)

// Copied from thrift tests
func TransportTest(t *testing.T, writeTrans thrift.TTransport, readTrans thrift.TTransport) {
	buf := make([]byte, TRANSPORT_BINARY_DATA_SIZE)
	if !writeTrans.IsOpen() {
		t.Fatalf("Transport %T not open: %s", writeTrans, writeTrans)
	}
	if !readTrans.IsOpen() {
		t.Fatalf("Transport %T not open: %s", readTrans, readTrans)
	}
	_, err := writeTrans.Write(transport_bdata)
	if err != nil {
		t.Fatalf("Transport %T cannot write binary data of length %d: %s", writeTrans, len(transport_bdata), err)
	}
	err = writeTrans.Flush(context.Background())
	if err != nil {
		t.Fatalf("Transport %T cannot flush write of binary data: %s", writeTrans, err)
	}
	n, err := io.ReadFull(readTrans, buf)
	if err != nil {
		t.Errorf("Transport %T cannot read binary data of length %d: %s", readTrans, TRANSPORT_BINARY_DATA_SIZE, err)
	}
	if n != TRANSPORT_BINARY_DATA_SIZE {
		t.Errorf("Transport %T read only %d instead of %d bytes of binary data", readTrans, n, TRANSPORT_BINARY_DATA_SIZE)
	}
	for k, v := range buf {
		if v != transport_bdata[k] {
			t.Fatalf("Transport %T read %d instead of %d for index %d of binary data 2", readTrans, v, transport_bdata[k], k)
		}
	}
	_, err = writeTrans.Write(transport_bdata)
	if err != nil {
		t.Fatalf("Transport %T cannot write binary data 2 of length %d: %s", writeTrans, len(transport_bdata), err)
	}
	err = writeTrans.Flush(context.Background())
	if err != nil {
		t.Fatalf("Transport %T cannot flush write binary data 2: %s", writeTrans, err)
	}
	buf = make([]byte, TRANSPORT_BINARY_DATA_SIZE)
	read := 1
	for n = 0; n < TRANSPORT_BINARY_DATA_SIZE && read != 0; {
		read, err = readTrans.Read(buf[n:])
		if err != nil {
			t.Errorf("Transport %T cannot read binary data 2 of total length %d from offset %d: %s", readTrans, TRANSPORT_BINARY_DATA_SIZE, n, err)
		}
		n += read
	}
	if n != TRANSPORT_BINARY_DATA_SIZE {
		t.Errorf("Transport %T read only %d instead of %d bytes of binary data 2", readTrans, n, TRANSPORT_BINARY_DATA_SIZE)
	}
	for k, v := range buf {
		if v != transport_bdata[k] {
			t.Fatalf("Transport %T read %d instead of %d for index %d of binary data 2", readTrans, v, transport_bdata[k], k)
		}
	}
}
