package gosasl

import (
	"encoding/binary"
	"fmt"
	"github.com/beltran/gssapi"
	"log"
	"regexp"
)

// DEFAULT_MAX_LENGTH is the max length that will be requested in the negotiation
// It can be set with gssapiMechanism.MaxLength = 1000
const DEFAULT_MAX_LENGTH = 16384000

var (
	krbSPNHost = regexp.MustCompile(`\A[^/]+/(_HOST)([@/]|\z)`)
)

// AUTH if the flag used for just basic auth, no confidentiality
var AUTH = "auth"

// AUTH_INT is the flag for authentication and integrety
var AUTH_INT = "auth-int"

// AUTH_CONF is the flag for authentication and confidentiality. It
// the most secure option.
var AUTH_CONF = "auth-conf"

//QOP_TO_FLAG is a dict that translate the string flag name into the actual bit
// It can be used wiht gssapiMechanism.UserSelectQop = QOP_TO_FLAG[AUTH_CONF] | QOP_TO_FLAG[AUTH_INT]
var QOP_TO_FLAG = map[string]byte{
	AUTH:      1,
	AUTH_INT:  2,
	AUTH_CONF: 4,
}

// QOP is the byte that holds the QOP flags
type QOP []byte

// MechanismConfig is the configuration to use for mechanisms
type MechanismConfig struct {
	name               string
	score              int
	complete           bool
	hasInitialResponse bool
	allowsAnonymous    bool
	usesPlaintext      bool
	activeSafe         bool
	dictionarySafe     bool
	qop                QOP
	// It can be set with mechanism.getConfig().AuthorizationID = "authorizationId"
	AuthorizationID string
}

// Mechanism is the common interface for all mechanisms
type Mechanism interface {
	start() ([]byte, error)
	step(challenge []byte) ([]byte, error)
	encode(outgoing []byte) ([]byte, error)
	decode(incoming []byte) ([]byte, error)
	dispose()
	getConfig() *MechanismConfig
}

// AnonymousMechanism corresponds to NONE/ Anonymous SASL mechanism
type AnonymousMechanism struct {
	config *MechanismConfig
}

// NewAnonymousMechanism returns a new AnonymousMechanism
func NewAnonymousMechanism() *AnonymousMechanism {
	return &AnonymousMechanism{
		config: newDefaultConfig("Anonymous"),
	}
}

func (m *AnonymousMechanism) start() ([]byte, error) {
	return m.step(nil)
}

func (m *AnonymousMechanism) step([]byte) ([]byte, error) {
	m.config.complete = true
	return []byte("Anonymous, None"), nil
}

func (m *AnonymousMechanism) encode([]byte) ([]byte, error) {
	return nil, nil
}

func (m *AnonymousMechanism) decode([]byte) ([]byte, error) {
	return nil, nil
}

func (m *AnonymousMechanism) dispose() {}

func (m *AnonymousMechanism) getConfig() *MechanismConfig {
	return m.config
}

// PlainMechanism corresponds to PLAIN SASL mechanism
type PlainMechanism struct {
	mechanismConfig *MechanismConfig
	identity        string
	username        string
	password        string
}

// NewPlainMechanism returns a new PlainMechanism
func NewPlainMechanism(username string, password string) *PlainMechanism {
	return &PlainMechanism{
		mechanismConfig: newDefaultConfig("PLAIN"),
		username:        username,
		password:        password,
	}
}

func (m *PlainMechanism) start() ([]byte, error) {
	return m.step(nil)
}

func (m *PlainMechanism) step(challenge []byte) ([]byte, error) {
	m.mechanismConfig.complete = true
	var authID string

	if m.mechanismConfig.AuthorizationID != "" {
		authID = m.mechanismConfig.AuthorizationID
	} else {
		authID = m.identity
	}
	NULL := "\x00"
	return []byte(fmt.Sprintf("%s%s%s%s%s", authID, NULL, m.username, NULL, m.password)), nil
}

func (m *PlainMechanism) encode(outgoing []byte) ([]byte, error) {
	return outgoing, nil
}

func (m *PlainMechanism) decode(incoming []byte) ([]byte, error) {
	return incoming, nil
}

func (m *PlainMechanism) dispose() {
	m.password = ""
}

func (m *PlainMechanism) getConfig() *MechanismConfig {
	return m.mechanismConfig
}

// GSSAPIMechanism corresponds to GSSAPI SASL mechanism
type GSSAPIMechanism struct {
	config           *MechanismConfig
	host             string
	user             string
	service          string
	negotiationStage int
	context          *GSSAPIContext
	qop              byte
	supportedQop     uint8
	serverMaxLength  int
	UserSelectQop    uint8
	MaxLength        int
}

// NewGSSAPIMechanism returns a new GSSAPIMechanism
func NewGSSAPIMechanism(service string) (mechanism *GSSAPIMechanism, err error) {
	context := newGSSAPIContext()
	mechanism = &GSSAPIMechanism{
		config:           newDefaultConfig("GSSAPI"),
		service:          service,
		negotiationStage: 0,
		context:          context,
		supportedQop:     QOP_TO_FLAG[AUTH] | QOP_TO_FLAG[AUTH_CONF] | QOP_TO_FLAG[AUTH_INT],
		MaxLength:        DEFAULT_MAX_LENGTH,
		UserSelectQop:    QOP_TO_FLAG[AUTH] | QOP_TO_FLAG[AUTH_INT] | QOP_TO_FLAG[AUTH_CONF],
	}
	return
}

func (m *GSSAPIMechanism) start() ([]byte, error) {
	return m.step(nil)
}

func (m *GSSAPIMechanism) step(challenge []byte) ([]byte, error) {
	if m.negotiationStage == 0 {
		err := initClientContext(m.context, m.service+"/"+m.host, nil)
		m.negotiationStage = 1
		return m.context.token, err

	} else if m.negotiationStage == 1 {
		err := initClientContext(m.context, m.service+"/"+m.host, challenge)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		var srcName *gssapi.Name
		if m.context.contextId != nil {
			srcName, _, _, _, _, _, _, _ = m.context.contextId.InquireContext()
			if srcName != nil {
				m.user = srcName.String()
			}
		}
		if m.user != "" {
			// Check if the context is available. If the user has set the flags
			// it will fail, although at this point we could know that the negotiation won't succeed
			if !m.context.integAvail() && !m.context.confAvail() {
				log.Println("No security layer can be established, authentication is still possible")
			}
			m.negotiationStage = 2
		}
		return m.context.token, nil
	} else if m.negotiationStage == 2 {
		data, err := m.context.unwrap(challenge)
		if err != nil {
			return nil, err
		}
		if len(data) != 4 {
			return nil, fmt.Errorf("Decoded data should have length for at this stage")
		}
		qopBits := data[0]
		data[0] = 0
		m.serverMaxLength = int(binary.BigEndian.Uint32(data))
		if m.serverMaxLength == 0 {
			return nil, fmt.Errorf("The maximum packet length can't be zero. The server doesn't support GSSAPI")
		}

		m.qop, err = m.selectQop(qopBits)
		// The client doesn't support or want any of the security layers offered by the server
		if err != nil {
			m.MaxLength = 0
		}

		header := make([]byte, 4)
		maxLength := m.serverMaxLength
		if m.MaxLength < m.serverMaxLength {
			maxLength = m.MaxLength
		}

		headerInt := (uint(m.qop) << 24) | uint(maxLength)

		binary.BigEndian.PutUint32(header, uint32(headerInt))

		// FLAG_BYTE + 3 bytes of length + user or authority
		var name string
		if name = m.user; m.config.AuthorizationID != "" {
			name = m.config.AuthorizationID
		}
		out := append(header, []byte(name)...)
		wrappedOut, err := m.context.wrap(out, false)

		m.config.complete = true
		return wrappedOut, err
	}
	return nil, fmt.Errorf("Error, this code should be unreachable")
}

func (m *GSSAPIMechanism) selectQop(qopByte byte) (byte, error) {
	availableQops := m.UserSelectQop & m.supportedQop & qopByte
	for _, qop := range []byte{QOP_TO_FLAG[AUTH_CONF], QOP_TO_FLAG[AUTH_INT], QOP_TO_FLAG[AUTH]} {
		if qop&availableQops != 0 {
			return qop, nil
		}
	}
	return byte(0), fmt.Errorf("No qop satisfying all the conditions where found")
}

// replaceSPNHostWildcard substitutes the special string '_HOST' in the given
// SPN for the given (current) host.
func replaceSPNHostWildcard(spn, host string) string {
	res := krbSPNHost.FindStringSubmatchIndex(spn)
	if res == nil || res[2] == -1 {
		return spn
	}
	return spn[:res[2]] + host + spn[res[3]:]
}

func (m GSSAPIMechanism) encode(outgoing []byte) ([]byte, error) {
	if m.qop == QOP_TO_FLAG[AUTH] {
		return outgoing, nil
	} else {
		var conf_flag bool = false
		if m.qop == QOP_TO_FLAG[AUTH_CONF] {
			conf_flag = true
		}
		return m.context.wrap(deepCopy(outgoing), conf_flag)
	}
}

func (m GSSAPIMechanism) decode(incoming []byte) ([]byte, error) {
	if m.qop == QOP_TO_FLAG[AUTH] {
		return incoming, nil
	}
	return m.context.unwrap(deepCopy(incoming))
}

func deepCopy(original []byte) []byte {
	copied := make([]byte, len(original))
	for i, el := range original {
		copied[i] = el
	}
	return copied
}

func (m GSSAPIMechanism) dispose() {
	m.context.dispose()
}

func (m GSSAPIMechanism) getConfig() *MechanismConfig {
	return m.config
}

// Client is the entry point for usage of this library
type Client struct {
	host            string
	authorizationID string
	mechanism       Mechanism
}

func newDefaultConfig(name string) *MechanismConfig {
	return &MechanismConfig{
		name:               name,
		score:              0,
		complete:           false,
		hasInitialResponse: false,
		allowsAnonymous:    true,
		usesPlaintext:      true,
		activeSafe:         false,
		dictionarySafe:     false,
		qop:                nil,
		AuthorizationID:    "",
	}
}

// NewSaslClient creates a new client given a host and a mechanism
func NewSaslClient(host string, mechanism Mechanism) *Client {
	mech, ok := mechanism.(*GSSAPIMechanism)
	if ok {
		mech.host = host
	}
	return &Client{
		host:      host,
		mechanism: mechanism,
	}
}

// Start initializes the client and may generate the first challenge
func (client *Client) Start() ([]byte, error) {
	return client.mechanism.start()
}

// Step is used for the initial handshake
func (client *Client) Step(challenge []byte) ([]byte, error) {
	return client.mechanism.step(challenge)
}

// Complete returns true if the handshake has ended
func (client *Client) Complete() bool {
	return client.mechanism.getConfig().complete
}

// GetConfig returns the configuration of the mechanism
func (client *Client) GetConfig() *MechanismConfig {
	return client.mechanism.getConfig()
}

// Encode is applied on the outgoing bytes to secure them usually
func (client *Client) Encode(outgoing []byte) ([]byte, error) {
	return client.mechanism.encode(outgoing)
}

// Decode is used on the incoming data to produce the usable bytes
func (client *Client) Decode(incoming []byte) ([]byte, error) {
	return client.mechanism.decode(incoming)
}

// Dispose eliminates sensitive information
func (client *Client) Dispose() {
	client.mechanism.dispose()
}
