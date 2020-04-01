package MqModular

import (
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"iPublic/LoggerModular"
	"sync"
	"time"
)

type RabbCallBack func(data []byte) error

type RabbExchange struct {
	Name string
	Type string
}

type MqQueue struct {
	QueueObj *amqp.Queue
	Consumes []<-chan amqp.Delivery
}

type RabbServer struct {
	URL          string                   //地址
	conn         *amqp.Connection         //
	Channel      *amqp.Channel            // channel info
	timeDelay    time.Time                //
	exchangeLock sync.Mutex               //
	exchange     map[string]*RabbExchange //
	queueLock    sync.Mutex               //
	queues       map[string]*MqQueue      // 存储Queue
	logger       *logrus.Entry            //
	lock         sync.Mutex               //
	ConnOk       bool                     //
}

func (serv *RabbServer) InitConnection() error {
	var err error
	serv.ConnOk = false
	serv.queues = make(map[string]*MqQueue)
	serv.exchange = make(map[string]*RabbExchange)
	serv.logger = LoggerModular.GetLogger().WithFields(logrus.Fields{
		"Server": serv.URL,
	})
	err = serv.DailConnect()
	if err != nil {
		serv.logger.Errorf("Dial Error: %s", err.Error())
		return err
	} else {
		serv.logger.Info("Connect to RabbitMQ OK")
	}
	serv.Channel, err = serv.conn.Channel()
	if err != nil {
		serv.logger.Errorf("Open Channel Error: %s", err.Error())
		return err
	} else {
		serv.logger.Info("Create Channel to RabbitMQ OK")
	}
	serv.ConnOk = true
	return nil
}

func (serv *RabbServer) DailConnect() (err error) {
	if serv.URL != "" {
		ticker := time.NewTicker(time.Second * 15)
		for {
			serv.conn, err = amqp.Dial(serv.URL)
			if err != nil {
				serv.logger.Errorf("Dial Rabbit Error: %v. ", err)
				time.Sleep(time.Second * 5)
			} else {
				serv.logger.Info("Connect to RabbitMQ OK")
				break
			}
			select {
			case <-ticker.C:
				serv.logger.Errorf("Dial Failed: %s", err.Error())
			}
		}
	} else {
		serv.logger.Error("Loss uri config for server.")
		return errors.New("Loss uri config for server. ")
	}
	go func() {
		// 断开后，释放阻塞，重新拨号
		for {
			serv.logger.Error("Connecting unexpected closed, ReDial...", <-serv.conn.NotifyClose(make(chan *amqp.Error)))
			serv.ConnOk = false
			for {
				serv.conn, err = amqp.Dial(serv.URL)
				if err != nil {
					serv.logger.Error("MQ Dial failed, Retrying...")
					time.Sleep(time.Second * 5)
				} else {
					serv.logger.Info("MQ Dial OK, Rebuild channel...")
					err = serv.Channel.Close()
					serv.Channel, err = serv.conn.Channel()
					serv.ConnOk = true
					break
				}
			}
		}
	}()
	return nil
}

func (serv *RabbServer) DisConnect() (err error) {
	defer serv.logger.Warning("DisConnecting MQ..")
	if serv.conn != nil {
		err = serv.conn.Close()
		return nil
	}
	return nil
}
