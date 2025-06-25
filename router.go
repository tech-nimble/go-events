package events

import (
	"regexp"

	amqp "github.com/rabbitmq/amqp091-go"
)

const headerRouteKey = "type"

// Router маршрутизатор сообщений
type Router interface {
	Match(msg amqp.Delivery, route string) bool
}

// HeaderRouterOption опция для маршрутизатора по заголовку
type HeaderRouterOption interface {
	apply(r *HeaderRouter)
}

type hrOptionFunc func(r *HeaderRouter)

func (f hrOptionFunc) apply(r *HeaderRouter) {
	f(r)
}

// HeaderRouter определяет путь по заголовку
// По умолчанию смотрит на заголовок с ключом type
type HeaderRouter struct {
	key     string
	matcher Matcher
}

// NewHeaderRouter конструктор маршрутизатора по заголовку
func NewHeaderRouter(options ...HeaderRouterOption) *HeaderRouter {
	r := &HeaderRouter{
		key:     headerRouteKey,
		matcher: strictMatcher,
	}

	for _, option := range options {
		option.apply(r)
	}

	return r
}

// Match сравнивает путь сообщения с указанным
func (r *HeaderRouter) Match(msg amqp.Delivery, route string) bool {
	var msgRoute string

	if typeFromHeader, ok := msg.Headers[headerRouteKey]; ok {
		msgRoute = typeFromHeader.(string)
	}

	return r.matcher(msgRoute, route)
}

// WithHeaderRouterKey устанавливает ключ заголовка по которому будет происходить маршрутизация
func WithHeaderRouterKey(key string) HeaderRouterOption {
	return hrOptionFunc(func(r *HeaderRouter) {
		if key == "" {
			return
		}
		r.key = key
	})
}

// WithHeaderRouterRegexpMatcher устанавливает регулярное выражение для сравнения
func WithHeaderRouterRegexpMatcher() HeaderRouterOption {
	return hrOptionFunc(func(r *HeaderRouter) {
		r.matcher = regexpMatcher
	})
}

// WithHeaderRouterMatcher устанавливает функцию сравнения
func WithHeaderRouterMatcher(matcher Matcher) HeaderRouterOption {
	return hrOptionFunc(func(r *HeaderRouter) {
		r.matcher = matcher
	})
}

// RouterKeyRouterOption опция для маршрутизатора по заголовку
type RouterKeyRouterOption interface {
	apply(r *RouterKeyRouter)
}

type rkOptionFunc func(r *RouterKeyRouter)

func (f rkOptionFunc) apply(r *RouterKeyRouter) {
	f(r)
}

// RouterKeyRouter определяет путь по заголовку routing key
type RouterKeyRouter struct {
	matcher Matcher
}

// NewRouterKeyRouter конструктор маршрутизатора по routing key
func NewRouterKeyRouter(options ...RouterKeyRouterOption) *RouterKeyRouter {
	r := &RouterKeyRouter{
		matcher: strictMatcher,
	}

	for _, option := range options {
		option.apply(r)
	}

	return r
}

// Match сравнивает путь сообщения с указанным
func (r *RouterKeyRouter) Match(msg amqp.Delivery, route string) bool {
	return r.matcher(msg.RoutingKey, route)
}

// WithRouterKeyRegexpMatcher устанавливает регулярное выражение для сравнения
func WithRouterKeyRegexpMatcher() RouterKeyRouterOption {
	return rkOptionFunc(func(r *RouterKeyRouter) {
		r.matcher = regexpMatcher
	})
}

// WithRouterKeyMatcher устанавливает функцию сравнения
func WithRouterKeyMatcher(matcher Matcher) RouterKeyRouterOption {
	return rkOptionFunc(func(r *RouterKeyRouter) {
		r.matcher = matcher
	})
}

type Matcher func(msgRoute string, handlerRoute string) bool

var strictMatcher = func(msgRoute string, handlerRoute string) bool {
	return msgRoute == handlerRoute
}

var regexpMatcher = func(msgRoute string, handlerRoute string) bool {
	match, _ := regexp.MatchString(handlerRoute, msgRoute)

	return match
}
