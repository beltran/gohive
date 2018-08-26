package gosasl

import (
	"encoding/json"
	"fmt"
	"github.com/beltran/gssapi"
	"log"
	"os"
	"sync"
)

type GSSAPIContext struct {
	DebugLog       bool
	RunAsService   bool
	ServiceName    string
	ServiceAddress string

	gssapi.Options

	*gssapi.Lib `json:"-"`
	loadonce    sync.Once

	// Service credentials loaded from keytab
	credential     *gssapi.CredId
	token          []byte
	continueNeeded bool
	contextId      *gssapi.CtxId
	reqFlags       uint32
	availFlags     uint32
}

//
func newGSSAPIContext() *GSSAPIContext {
	var c = &GSSAPIContext{
		reqFlags: uint32(gssapi.GSS_C_INTEG_FLAG) + uint32(gssapi.GSS_C_MUTUAL_FLAG) + uint32(gssapi.GSS_C_SEQUENCE_FLAG) + uint32(gssapi.GSS_C_CONF_FLAG),
	}
	prefix := "gosasl-client"
	err := loadlib(c.DebugLog, prefix, c)
	if err != nil {
		log.Fatal(err)
	}

	j, _ := json.MarshalIndent(c, "", "  ")
	c.Debug(fmt.Sprintf("Config: %s", string(j)))
	return c
}

// InitClientContext initializes the context and gets the response(token)
// to send to the server
func initClientContext(c *GSSAPIContext, service string, inputToken []byte) error {
	c.ServiceName = service

	var _inputToken *gssapi.Buffer
	var err error
	if inputToken == nil {
		_inputToken = c.GSS_C_NO_BUFFER
	} else {
		_inputToken, err = c.MakeBufferBytes(inputToken)
		defer _inputToken.Release()
		if err != nil {
			return err
		}
	}

	preparedName := prepareServiceName(c)
	defer preparedName.Release()

	contextId, _, token, outputRetFlags, _, err := c.InitSecContext(
		nil,
		c.contextId,
		preparedName,
		c.GSS_MECH_KRB5,
		c.reqFlags,
		0,
		c.GSS_C_NO_CHANNEL_BINDINGS,
		_inputToken)
	defer token.Release()

	c.token = token.Bytes()
	c.contextId = contextId
	c.availFlags = outputRetFlags
	return nil
}

// Wrap calls GSS_Wrap
func (c *GSSAPIContext) wrap(original []byte, conf_flag bool) (wrapped []byte, err error) {
	if original == nil {
		return
	}
	_original, err := c.MakeBufferBytes(original)
	defer _original.Release()

	if err != nil {
		return nil, err
	}
	_, wrappedBuffer, err := c.contextId.Wrap(conf_flag, gssapi.GSS_C_QOP_DEFAULT, _original)
	defer wrappedBuffer.Release()
	if err != nil {
		return nil, err
	}
	return wrappedBuffer.Bytes(), nil
}

// Unwrap calls GSS_Unwrap
func (c *GSSAPIContext) unwrap(original []byte) (unwrapped []byte, err error) {
	if original == nil {
		return
	}
	_original, err := c.MakeBufferBytes(original)
	defer _original.Release()

	if err != nil {
		return nil, err
	}
	unwrappedBuffer, _, _, err := c.contextId.Unwrap(_original)
	defer unwrappedBuffer.Release()
	if err != nil {
		return nil, err
	}
	return unwrappedBuffer.Bytes(), nil
}

// Dispose releases the acquired memory and destroys sensitive information
func (c *GSSAPIContext) dispose() error {
	if c.contextId != nil {
		return c.contextId.Unload()
	}
	return nil
}

// IntegAvail returns true in the integ_flag is available and therefore a security layer can be established
func (c *GSSAPIContext) integAvail() bool {
	return c.availFlags&uint32(gssapi.GSS_C_INTEG_FLAG) != 0
}

// ConfAvail returns true in the conf_flag is available and therefore a confidentiality layer can be established
func (c *GSSAPIContext) confAvail() bool {
	return c.availFlags&uint32(gssapi.GSS_C_CONF_FLAG) != 0
}

func loadlib(debug bool, prefix string, c *GSSAPIContext) error {
	max := gssapi.Err + 1
	if debug {
		max = gssapi.MaxSeverity
	}
	pp := make([]gssapi.Printer, 0, max)
	for i := gssapi.Severity(0); i < max; i++ {
		p := log.New(os.Stderr,
			fmt.Sprintf("%s: %s\t", prefix, i),
			log.LstdFlags)
		pp = append(pp, p)
	}
	c.Options.Printers = pp

	lib, err := gssapi.Load(&c.Options)
	if err != nil {
		return err
	}
	c.Lib = lib
	return nil
}

func prepareServiceName(c *GSSAPIContext) *gssapi.Name {
	if c.ServiceName == "" {
		log.Fatal("Need a --service-name")
	}

	nameBuf, err := c.MakeBufferString(c.ServiceName)
	defer nameBuf.Release()
	if err != nil {
		log.Fatal(err)
	}

	name, err := nameBuf.Name(c.GSS_KRB5_NT_PRINCIPAL_NAME)
	if err != nil {
		log.Fatal(err)
	}
	if name.String() != c.ServiceName {
		log.Fatalf("name: got %q, expected %q", name.String(), c.ServiceName)
	}

	return name
}
