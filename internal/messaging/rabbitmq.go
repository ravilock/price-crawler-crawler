package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrUnroutable = errors.New("RabbitMQ returned an unroutable message")

const MaxRequestTimeout = 30 * time.Second

func WithBoundedRequestTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if timeout <= 0 || timeout > MaxRequestTimeout {
		return nil, nil, fmt.Errorf("request timeout must be positive and no greater than %s", MaxRequestTimeout)
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}

// Publisher sends persistent messages and waits for broker confirms.
type Publisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	confirms   <-chan amqp.Confirmation
	returns    <-chan amqp.Return
	mu         sync.Mutex
}

func NewPublisher(url string) (*Publisher, error) {
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ publisher channel: %w", err)
	}
	if err = channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	return &Publisher{
		connection: connection,
		channel:    channel,
		confirms:   channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:    channel.NotifyReturn(make(chan amqp.Return, 1)),
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, queue string, envelope Envelope) error {
	if queue == "" {
		return errors.New("queue is required")
	}
	body, err := envelope.Encode()
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err = p.channel.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     envelope.MessageID,
		CorrelationId: envelope.CorrelationID,
		Type:          envelope.MessageType,
		Body:          body,
	}); err != nil {
		return fmt.Errorf("publish RabbitMQ message: %w", err)
	}
	select {
	case confirmation, ok := <-p.confirms:
		if !ok {
			return errors.New("RabbitMQ publisher confirmation channel closed")
		}
		if !confirmation.Ack {
			return errors.New("RabbitMQ negatively acknowledged the message")
		}
		select {
		case returned := <-p.returns:
			return fmt.Errorf("%w: queue %s", ErrUnroutable, returned.RoutingKey)
		default:
			return nil
		}
	case <-ctx.Done():
		return fmt.Errorf("waiting for RabbitMQ publisher confirmation: %w", ctx.Err())
	}
}

func (p *Publisher) RequestReply(parent context.Context, queue string, request Envelope, timeout time.Duration) (Envelope, error) {
	if queue == "" {
		return Envelope{}, errors.New("queue is required")
	}
	ctx, cancel, err := WithBoundedRequestTimeout(parent, timeout)
	if err != nil {
		return Envelope{}, err
	}
	defer cancel()
	body, err := request.Encode()
	if err != nil {
		return Envelope{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	replyQueue, err := p.channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return Envelope{}, fmt.Errorf("declare exclusive reply queue: %w", err)
	}
	replies, err := p.channel.Consume(replyQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		return Envelope{}, fmt.Errorf("consume reply queue: %w", err)
	}
	err = p.channel.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     request.MessageID,
		CorrelationId: request.CorrelationID,
		ReplyTo:       replyQueue.Name,
		Type:          request.MessageType,
		Body:          body,
	})
	if err != nil {
		return Envelope{}, fmt.Errorf("publish RabbitMQ request: %w", err)
	}
	select {
	case confirmation, ok := <-p.confirms:
		if !ok || !confirmation.Ack {
			return Envelope{}, errors.New("RabbitMQ did not confirm the request")
		}
	case <-ctx.Done():
		return Envelope{}, fmt.Errorf("waiting for RabbitMQ request confirmation: %w", ctx.Err())
	}
	select {
	case returned := <-p.returns:
		return Envelope{}, fmt.Errorf("%w: queue %s", ErrUnroutable, returned.RoutingKey)
	default:
	}
	for {
		select {
		case delivery, ok := <-replies:
			if !ok {
				return Envelope{}, errors.New("RabbitMQ reply queue closed")
			}
			if delivery.CorrelationId != request.CorrelationID {
				continue
			}
			return DecodeDelivery(delivery)
		case <-ctx.Done():
			return Envelope{}, fmt.Errorf("waiting for RabbitMQ reply: %w", ctx.Err())
		}
	}
}

func (p *Publisher) Close() error {
	channelErr := p.channel.Close()
	connectionErr := p.connection.Close()
	if channelErr != nil {
		return channelErr
	}
	return connectionErr
}

// Consumer owns a durable queue and exposes deliveries for manual ack after durable processing.
type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
}

func NewConsumer(url, queue, consumerTag string, prefetch int) (*Consumer, error) {
	if queue == "" || prefetch < 1 {
		return nil, errors.New("queue and positive prefetch are required")
	}
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ consumer channel: %w", err)
	}
	if err = declareQueueTopology(channel, queue); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, err
	}
	if err = channel.Qos(prefetch, 0, false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("set RabbitMQ prefetch: %w", err)
	}
	deliveries, err := channel.Consume(queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("consume from queue %q: %w", queue, err)
	}
	return &Consumer{connection: connection, channel: channel, deliveries: deliveries}, nil
}

func declareQueueTopology(channel *amqp.Channel, queue string) error {
	deadExchange := "price.dead-letter"
	deadQueue := queue + ".dead"
	if err := channel.ExchangeDeclare(deadExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(deadQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter queue %q: %w", deadQueue, err)
	}
	if err := channel.QueueBind(deadQueue, deadQueue, deadExchange, false, nil); err != nil {
		return fmt.Errorf("bind dead-letter queue %q: %w", deadQueue, err)
	}
	args := amqp.Table{
		"x-dead-letter-exchange":    deadExchange,
		"x-dead-letter-routing-key": deadQueue,
	}
	if _, err := channel.QueueDeclare(queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare durable queue %q: %w", queue, err)
	}
	return nil
}

func (c *Consumer) Deliveries() <-chan amqp.Delivery { return c.deliveries }

// Close leaves unacknowledged deliveries available for broker redelivery.
func (c *Consumer) Close() error {
	channelErr := c.channel.Close()
	connectionErr := c.connection.Close()
	if channelErr != nil {
		return channelErr
	}
	return connectionErr
}

func DecodeDelivery(delivery amqp.Delivery) (Envelope, error) {
	return Decode(delivery.Body)
}
