package rabbitmq

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"github.com/tech-nimble/go-events"
)

const (
	reconnectDelay = 5
)

var (
	errHandlerNameBusy   = errors.New("Обработчик с таким именем уже зарегистрирован ")
	errChannelNotCreated = errors.New("Канал не создан ")
)

// ConsumerHandle функция которая примет сообщение
type ConsumerHandle func(ctx context.Context, msg amqp.Delivery)

type ConsumerHandler struct {
	name   string
	handle ConsumerHandle
}

type ConsumerOptions struct {
	// Consumer sets the consumer tag used when consuming.
	Consumer string

	// AutoAck sets the auto-ack flag. When this is set to false, you must
	// manually ack any deliveries. This is always true for the Client when
	// consuming replies.
	AutoAck bool

	// Exclusive sets the exclusive flag. When this is set to true, no other
	// instances can consume from a given queue. This has no affect on the
	// Client when consuming replies where it's always set to true so that no
	// two clients can consume from the same reply-to queue.
	Exclusive bool

	// QoSPrefetchCount sets the prefetch-count. Set this to a value to ensure
	// that amqp-rpc won't prefetch all messages in the queue. This has no
	// effect on the Client which will always try to fetch everything.
	QoSPrefetchCount int

	// QoSPrefetchSize sets the prefetch-size. Set this to a value to ensure
	// that amqp-rpc won't prefetch all messages in the queue.
	QoSPrefetchSize int

	// Args sets the arguments table used.
	Args amqp.Table
}

// Consumer подписывается на сообщения
// Создает очередь и привязывает её к exchange, если указан
type Consumer struct {
	rabbitmq *RabbitMQ
	Handlers []*ConsumerHandler

	consumerOptions *ConsumerOptions
	queueOptions    *QueueOptions
	bindOptions     []*BindOptions
	router          events.Router
}

func NewConsumer(url string) *Consumer {
	return &Consumer{
		rabbitmq: NewRabbitMQ(&Options{
			Url:             url,
			ConnectAttempts: 5,
		}),
		router: events.NewHeaderRouter(),
	}
}

func NewConsumerWithClient(rabbitmq *RabbitMQ) *Consumer {
	return &Consumer{
		rabbitmq: rabbitmq,
		router:   events.NewHeaderRouter(),
	}
}

func (c *Consumer) ListenAndServe() error {
	if err := c.init(); err != nil {
		return err
	}

	for range c.Handlers {
		go c.listen()
	}

	return nil
}

func (c *Consumer) Close() {
	if c.rabbitmq != nil {
		c.rabbitmq.Close()
	}
}

func (c *Consumer) init() error {
	if err := c.rabbitmq.Init(); err != nil {
		return err
	}

	ch := c.rabbitmq.GetChannel()
	if ch == nil {
		return errChannelNotCreated
	}

	if err := ch.Qos(
		c.getQoSPrefetchCount(),
		c.getQoSPrefetchSize(),
		false,
	); err != nil {
		return err
	}

	if err := declareQueueAndBind(ch, c.queueOptions, c.bindOptions); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) listen() {
	for {
		c.subscribe()
		time.Sleep(time.Duration(reconnectDelay) * time.Second)
	}
}

func (c *Consumer) subscribe() {
	ch := c.rabbitmq.GetChannel()
	if ch == nil {
		log.Error().Err(errChannelNotCreated).Msg("failed subscribe")
		return
	}

	msgs, err := ch.Consume(
		c.getQueueName(),
		c.getName(),
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Error().Err(err).Msg("Failed consume")
		return
	}

	for msg := range msgs {
		c.handle(msg)
	}
}

func (c *Consumer) handle(msg amqp.Delivery) {
	defer recovery(msg)

	ctx := context.Background()

	for _, h := range c.Handlers {
		if c.router.Match(msg, h.name) {
			h.handle(ctx, msg)
			return
		}
	}

	if err := msg.Nack(false, false); err != nil {
		log.Error().Err(err).Msg("не удалось ответить об отрицательной обрабоки сообщения")
	}
}

func (c *Consumer) AddHandler(name string, handle ConsumerHandle) error {
	for _, h := range c.Handlers {
		if h.name == name {
			return errHandlerNameBusy
		}
	}

	c.Handlers = append(c.Handlers, &ConsumerHandler{
		name:   name,
		handle: handle,
	})

	return nil
}

func (c *Consumer) SetRouter(router events.Router) {
	c.router = router
}

func (c *Consumer) WithConsumerOptions(settings *ConsumerOptions) *Consumer {
	c.consumerOptions = settings

	return c
}

func (c *Consumer) WithQueueOptions(settings *QueueOptions) *Consumer {
	c.queueOptions = settings

	return c
}

func (c *Consumer) WithBindOptions(settings *BindOptions) *Consumer {
	c.bindOptions = append(c.bindOptions, settings)

	return c
}

func (c *Consumer) getQueueName() string {
	return c.queueOptions.Name
}

func (c *Consumer) getName() string {
	return c.rabbitmq.GetName()
}

func (c *Consumer) getQoSPrefetchCount() int {
	if c.consumerOptions.QoSPrefetchCount != 0 {
		return c.consumerOptions.QoSPrefetchCount
	}

	return 1
}

func (c *Consumer) getQoSPrefetchSize() int {
	return c.consumerOptions.QoSPrefetchSize
}

func (c *Consumer) GetRabbitMQ() *RabbitMQ {
	return c.rabbitmq
}

func recovery(msg amqp.Delivery) {
	if errRecover := recover(); errRecover != nil {
		if isBrokenPipeError(errRecover) {
			panic(errRecover)
		}

		if err, ok := errRecover.(error); ok {
			log.Error().Err(err).Msgf("Recovering from panic: routing_key: %s, exchange: %s", msg.RoutingKey, msg.Exchange)
		}

		if err := msg.Reject(true); err != nil {
			log.Error().Err(err).Msg("не удалось ответить об отрицательной обработки сообщения")
		}
	}
}

func isBrokenPipeError(err interface{}) bool {
	if ne, okOpError := err.(*net.OpError); okOpError {
		if se, okSyscallError := ne.Err.(*os.SyscallError); okSyscallError {
			if strings.Contains(strings.ToLower(se.Error()), "broken pipe") {
				return true
			}

			if strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
				return true
			}
		}
	}

	return false
}
