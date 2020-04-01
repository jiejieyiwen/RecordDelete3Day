package MqModular

import (
	"github.com/streadway/amqp"
	"sync"
)

type RabbServerInterface interface {
	InitConnection() error                                                                   //初始化链接
	QueueDeclare(q_name string, exClusive bool, durable bool) (queue *amqp.Queue, err error) //声明Queue，已存在时不影响
	AddConsumer(tag string, queue *amqp.Queue) (err error)                                   // 添加消费者
	HandleMessage(deliveries <-chan amqp.Delivery, callback RabbCallBack)                    // 消息处理
	BindExchangeQueue(queue *amqp.Queue, exchange string, route_key string) (err error)      //绑定Exchange与Queue
	GetRabbitMQServ(Url string, wg *sync.WaitGroup, serv *RabbServer) error
}
