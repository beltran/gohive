// +build !kerberos

package gosasl

// GSSAPIMechanism corresponds to GSSAPI SASL mechanism
type GSSAPIMechanism struct {
	host string
}

const errorMsg string = "gosasl was installed without kerberos support please reinstall with `go get` using the flags `build kerberos`"

// NewGSSAPIMechanism returns a new GSSAPIMechanism
func NewGSSAPIMechanism(service string) (mechanism *GSSAPIMechanism, err error) {
	panic(errorMsg)
}

func (m *GSSAPIMechanism) start() ([]byte, error) {
	panic(errorMsg)
	return nil, nil
}

func (m *GSSAPIMechanism) step(challenge []byte) ([]byte, error) {
	panic(errorMsg)
	return nil, nil
}

func (m GSSAPIMechanism) encode(outgoing []byte) ([]byte, error) {
	panic(errorMsg)
	return nil, nil
}

func (m GSSAPIMechanism) decode(incoming []byte) ([]byte, error) {
	panic(errorMsg)
	return nil, nil
}

func (m GSSAPIMechanism) dispose() {
	panic(errorMsg)
}

func (m GSSAPIMechanism) getConfig() *MechanismConfig {
	panic(errorMsg)
	return nil
}
