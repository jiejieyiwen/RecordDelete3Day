package MqModular

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"iPublic/Utils"
	"os"
	"strings"
	"time"
)

const (
	MIME_Json  = "application/json"
	MIME_Plain = "text/plain"

	Kind_Direct  = "direct"
	Kind_Fanout  = "fanout"
	Kind_Topic   = "topic"
	Kind_Headers = "headers"
)

func (serv *RabbServer) AddExChange(name, Type string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	serv.exchangeLock.Lock()
	defer serv.exchangeLock.Unlock()
	serv.exchange[name] = &RabbExchange{Name: name, Type: Type}
}

func (serv *RabbServer) QueueDeclare(q_name string, exClusive bool, durable bool) (queue *MqQueue, err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if serv.ConnOk {
		if serv.Channel == nil {
			fmt.Println("Server Channel Get Error. ")
			return nil, errors.New("Server Channel Get Error. ")
		}
		_queue, err := serv.Channel.QueueDeclare(
			q_name,    // name of the queue
			durable,   // 持久化
			false,     // 自动删除,临时队列
			exClusive, // 排他队列,仅对首次声明它的连接可见，并在连接断开时自动删除
			false,     // noWait
			nil,       // arguments
		)
		if err != nil {
			return nil, err
		} else {
			serv.queueLock.Lock()
			var queue MqQueue
			queue.QueueObj = &_queue
			serv.queues[q_name] = &queue
			serv.queueLock.Unlock()
			return &queue, nil
		}
	} else {
		return nil, errors.New("Connection Loss")
	}
}

func (serv *RabbServer) GetOrCreateQueue(q_name string, exClusive bool, durable bool) (*MqQueue, error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	if queue, ok := serv.queues[q_name]; ok {
		return queue, nil
	} else {
		Retry := 0
		for {
			queue, err := serv.QueueDeclare(q_name, exClusive, durable)
			if (err == nil && queue != nil) || Retry >= 5 {
				if queue == nil {
					return queue, errors.New("Can't Create Queue, Error ")
				}
				return queue, err
			} else {
				Retry++
				time.Sleep(2 * time.Second)
			}
		}
	}
}

func (serv *RabbServer) AddConsumer(tag string, queue *MqQueue) (err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if len(tag) == 0 {
		host, err := Utils.GetLocalHostName()
		proce := strings.Split(os.Args[0], "/")
		if err == nil {
			tag = fmt.Sprintf("GoLang.%s.%s", host, proce[len(proce)-1:][0])
		} else {
			tag = fmt.Sprintf("GoLang.%s.%s", "Null", proce[len(proce)-1:][0])
		}
	}
	if serv.ConnOk {
		deliveries, err := serv.Channel.Consume(
			queue.QueueObj.Name, // name
			tag,                 // consumerTag,
			false,               // noAck
			false,               // exclusive
			false,               // noLocal
			false,               // noWait
			nil,                 // arguments
		)
		queue.Consumes = append(queue.Consumes, deliveries)
		return err
	} else {
		return errors.New("Connection loss. ")
	}
}

func (serv *RabbServer) HandleMessage(deliveries <-chan amqp.Delivery, callback RabbCallBack) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	//serv.lock.Lock()
	//defer serv.lock.Unlock()
	for d := range deliveries {
		serv.logger.Infof(
			"[%v] got %dB delivery >> ID:[%v] from exchange[%v]. data: %q",
			d.ConsumerTag, len(d.Body), d.DeliveryTag, d.Exchange, d.Body,
		)
		err := callback(d.Body)
		if err != nil {
			serv.logger.Errorf("HandleMessage CallBack Failed: %s", err.Error())
		} else {
			err = d.Ack(false)
			if err != nil {
				serv.logger.Errorf("Ack ID:[%v] Failed", d.DeliveryTag)
			}
		}
	}
	serv.logger.Info("handle: deliveries channel closed")
}

func (serv *RabbServer) BindExchangeQueue(queue *amqp.Queue, exchange string, RoutingKey string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if serv.ConnOk {
		if err = serv.Channel.QueueBind(
			queue.Name, // name of the queue
			RoutingKey, // RoutingKey
			exchange,   // sourceExchange
			false,      // noWait
			nil,        // arguments
		); err != nil {
			serv.logger.Errorf("Queue Bind: %s ", err.Error())
			return err
		} else {
			serv.AddExChange(exchange, Kind_Direct)
			return nil
		}

	} else {
		return errors.New("Connection Loss. ")
	}

}

func (serv *RabbServer) UnBindExchangeQueue(queue *amqp.Queue, exchange string, route_key string) (err error) {
	/*
		程序退出时，解绑
	*/
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if serv.ConnOk {
		if err = serv.Channel.QueueUnbind(
			queue.Name, // name of the queue
			route_key,  // bindingKey
			exchange,   // sourceExchange
			nil,        // noWait
		); err != nil {
			serv.logger.Errorf("Queue UnBind: %s ", err.Error())
			return err
		} else {
			serv.AddExChange(exchange, Kind_Direct)
			return nil
		}

	} else {
		return errors.New("Connection Loss. ")
	}
}

func (serv *RabbServer) PublishMessage(ch *amqp.Channel, q *amqp.Queue, body []byte) (err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if serv.ConnOk {
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: MIME_Plain,
				Body:        body,
			})
		if err != nil {
			return err
		} else {
			//fmt.Println("Send Message, len ", len(body))
			return nil
		}
	} else {
		return errors.New("Rabbit Connection broken.")
	}

}

func (serv *RabbServer) PublishMessageJson(ch *amqp.Channel, q *amqp.Queue, body []byte) (err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if serv.ConnOk {
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: MIME_Json,
				Body:        body,
			})
		if err != nil {
			return err
		} else {
			//fmt.Println("Send Message, len ", len(body))
			return nil
		}
	} else {
		return errors.New("Rabbit Connection broken.")
	}

}

func (serv *RabbServer) ExchangeDeclare(name string, kind string, durable, autoDelete, internal, noWait bool) error {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	args := amqp.Table{}
	if err := serv.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args); err != nil {
		return err
	} else {
		serv.AddExChange(name, kind)
		return nil
	}
}

func (serv *RabbServer) GetOrCreateExchange(name string, kind string, durable, autoDelete, internal, noWait bool) error {
	if _, ok := serv.exchange[name]; ok {
		return nil
	} else {
		Retry := 0
		for {
			err := serv.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait)
			if err == nil || Retry >= 5 {
				return err
			} else {
				Retry++
				time.Sleep(2 * time.Second)
			}
		}
	}
}

func (serv *RabbServer) PublishMessageTopic(ch *amqp.Channel, exchange, routing string, cType string, body []byte) (err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	serv.lock.Lock()
	defer serv.lock.Unlock()
	if serv.ConnOk {
		err = ch.Publish(
			exchange, // exchange
			routing,  // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				ContentType: cType,
				Body:        body,
			})
		if err != nil {
			return err
		} else {
			return nil
		}
	} else {
		return errors.New("Rabbit Connection broken. ")
	}
}
