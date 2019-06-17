package pusher

import (
	"github.com/streadway/amqp"
	"log"
)

type Message struct {
	Uid     string
	Payload []byte
}

type QueueConfig interface {
	WaitToConnect() *amqp.Connection
}

type Pusher struct {
	exchange  string
	conf      QueueConfig
	writeCH   chan *Message
	LogPrefix string
}

func (q *Pusher) Background() chan struct{} {
	ch := make(chan struct{})
	go q.realBackground(ch)
	return ch
}

func (q *Pusher) realBackground(stopCH chan struct{}) {
	for {
		err := q.processing(stopCH)
		if err == nil {
			return
		}
		log.Println(q.LogPrefix, err)
	}
}

func (q *Pusher) processing(stopCH chan struct{}) error {
	conn := q.conf.WaitToConnect()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	err = ch.ExchangeDeclare(
		q.exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	closedCH := make(chan *amqp.Error)
	ch.NotifyClose(closedCH)
	for {
		select {
		case <-stopCH:
			return nil
		case err := <-closedCH:
			return err
		case msg := <-q.writeCH:
			err := ch.Publish(q.exchange, "", false, false, amqp.Publishing{
				CorrelationId: msg.Uid,
				Body:          msg.Payload,
			})
			if err != nil {
				log.Println(q.LogPrefix, "publish", err)
			}
		}
	}
}

func (q *Pusher) SendMessage(msg *Message) {
	q.writeCH <- msg
}

func NewPusher(conf QueueConfig, bufferSize int, exchange string) (*Pusher, error) {
	return &Pusher{
		conf:      conf,
		LogPrefix: "[PUSHER]",
		exchange:  exchange,
		writeCH:   make(chan *Message, bufferSize),
	}, nil
}
