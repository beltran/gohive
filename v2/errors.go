package gohive

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/apache/thrift/lib/go/thrift"
)

type errBadConn struct {
	cause error
}

func (e *errBadConn) Error() string { return e.cause.Error() }
func (e *errBadConn) Unwrap() error { return e.cause }
func (e *errBadConn) Is(target error) bool {
	return target == driver.ErrBadConn
}

// errorClassifier inspects an error and returns the appropriate sentinel error
// if it matches, or nil if it does not match.
type errorClassifier func(err error) error

// classifiers is the ordered list of error classifiers. Each function receives
// the raw error and returns the sentinel it maps to, or nil to pass through to
// the next classifier. Add new entries here to extend error classification.
var classifiers = []errorClassifier{
	// --- driver.ErrBadConn: connection is dead ---

	// Thrift transport not open.
	func(err error) error {
		var te thrift.TTransportException
		if errors.As(err, &te) && te.TypeId() == thrift.NOT_OPEN {
			return driver.ErrBadConn
		}
		return nil
	},
	// Thrift end of file (peer closed).
	func(err error) error {
		var te thrift.TTransportException
		if errors.As(err, &te) && te.TypeId() == thrift.END_OF_FILE {
			return driver.ErrBadConn
		}
		return nil
	},

	// --- Standard io errors ---

	// io.EOF
	func(err error) error {
		if errors.Is(err, io.EOF) {
			return driver.ErrBadConn
		}
		return nil
	},
	// io.ErrUnexpectedEOF
	func(err error) error {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return driver.ErrBadConn
		}
		return nil
	},

	// --- Network / syscall errors ---

	// net: use of closed connection
	func(err error) error {
		if errors.Is(err, net.ErrClosed) {
			return driver.ErrBadConn
		}
		return nil
	},
	// syscall: connection reset by peer
	func(err error) error {
		if errors.Is(err, syscall.ECONNRESET) {
			return driver.ErrBadConn
		}
		return nil
	},
	// syscall: broken pipe
	func(err error) error {
		if errors.Is(err, syscall.EPIPE) {
			return driver.ErrBadConn
		}
		return nil
	},
	// syscall: connection refused
	func(err error) error {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return driver.ErrBadConn
		}
		return nil
	},
	// syscall: connection aborted
	func(err error) error {
		if errors.Is(err, syscall.ECONNABORTED) {
			return driver.ErrBadConn
		}
		return nil
	},

	// --- HiveServer2 / Impala application-level errors ---

	// TStatusCode INVALID_HANDLE_STATUS (session/operation handle not recognized)
	func(err error) error {
		if strings.Contains(err.Error(), "INVALID_HANDLE_STATUS") {
			return driver.ErrBadConn
		}
		return nil
	},
	// HiveServer2: invalid session handle
	func(err error) error {
		if strings.Contains(err.Error(), "Invalid SessionHandle") {
			return driver.ErrBadConn
		}
		return nil
	},
	// Impala: invalid session id
	func(err error) error {
		if strings.Contains(err.Error(), "Invalid session id") {
			return driver.ErrBadConn
		}
		return nil
	},
	// HiveServer2: session does not exist
	func(err error) error {
		if strings.Contains(err.Error(), "Session does not exist") {
			return driver.ErrBadConn
		}
		return nil
	},
}

// classifyError runs the raw error through all classifiers and returns the
// first matching sentinel. If no classifier matches, the original error is
// returned unchanged.
func classifyError(err error) error {
	if err == nil {
		return nil
	}
	for _, classify := range classifiers {
		if sentinel := classify(err); sentinel != nil {
			if sentinel == driver.ErrBadConn {
				return &errBadConn{cause: err}
			}
			return sentinel
		}
	}
	return err
}
