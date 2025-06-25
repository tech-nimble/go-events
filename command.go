package events

type Command interface {
	GetBody() ([]byte, error)
	GetExchangeName() string
	GetCommandName() string
	GetHeaders() map[string]interface{}
	SetHeaders(map[string]interface{})
}

type Response interface {
	GetBody() ([]byte, error)
	GetHeaders() map[string]interface{}
	SetHeaders(map[string]interface{})
}
