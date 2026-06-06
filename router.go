// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package events

import (
	"regexp"

	amqp "github.com/rabbitmq/amqp091-go"
)

const headerRouteKey = "type"

// Router matches a message against a route.
type Router interface {
	Match(msg amqp.Delivery, route string) bool
}

// HeaderRouterOption configures a HeaderRouter.
type HeaderRouterOption interface {
	apply(r *HeaderRouter)
}

type hrOptionFunc func(r *HeaderRouter)

func (f hrOptionFunc) apply(r *HeaderRouter) {
	f(r)
}

// HeaderRouter routes by a message header.
// By default it inspects the "type" header.
type HeaderRouter struct {
	key     string
	matcher Matcher
}

// NewHeaderRouter builds a header-based router.
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

// Match reports whether the message matches the given route.
func (r *HeaderRouter) Match(msg amqp.Delivery, route string) bool {
	var msgRoute string

	if typeFromHeader, ok := msg.Headers[headerRouteKey]; ok {
		msgRoute = typeFromHeader.(string)
	}

	return r.matcher(msgRoute, route)
}

// WithHeaderRouterKey sets the header key used for routing.
func WithHeaderRouterKey(key string) HeaderRouterOption {
	return hrOptionFunc(func(r *HeaderRouter) {
		if key == "" {
			return
		}
		r.key = key
	})
}

// WithHeaderRouterRegexpMatcher matches routes using regular expressions.
func WithHeaderRouterRegexpMatcher() HeaderRouterOption {
	return hrOptionFunc(func(r *HeaderRouter) {
		r.matcher = regexpMatcher
	})
}

// WithHeaderRouterMatcher sets a custom match function.
func WithHeaderRouterMatcher(matcher Matcher) HeaderRouterOption {
	return hrOptionFunc(func(r *HeaderRouter) {
		r.matcher = matcher
	})
}

// RouterKeyRouterOption configures a RouterKeyRouter.
type RouterKeyRouterOption interface {
	apply(r *RouterKeyRouter)
}

type rkOptionFunc func(r *RouterKeyRouter)

func (f rkOptionFunc) apply(r *RouterKeyRouter) {
	f(r)
}

// RouterKeyRouter routes by the AMQP routing key.
type RouterKeyRouter struct {
	matcher Matcher
}

// NewRouterKeyRouter builds a routing-key based router.
func NewRouterKeyRouter(options ...RouterKeyRouterOption) *RouterKeyRouter {
	r := &RouterKeyRouter{
		matcher: strictMatcher,
	}

	for _, option := range options {
		option.apply(r)
	}

	return r
}

// Match reports whether the message matches the given route.
func (r *RouterKeyRouter) Match(msg amqp.Delivery, route string) bool {
	return r.matcher(msg.RoutingKey, route)
}

// WithRouterKeyRegexpMatcher matches routes using regular expressions.
func WithRouterKeyRegexpMatcher() RouterKeyRouterOption {
	return rkOptionFunc(func(r *RouterKeyRouter) {
		r.matcher = regexpMatcher
	})
}

// WithRouterKeyMatcher sets a custom match function.
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
