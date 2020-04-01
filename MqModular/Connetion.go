package MqModular

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"iPublic/LoggerModular"
	"math/rand"
	"sync"
	"time"
)

/*
	连接池
*/

type RichConn struct {
	Serv *RabbServer
}

type ConnPool struct {
	cond      *sync.Cond
	ConnURL   string
	MaxPool   int         // 最大链接池大小
	poolLock  sync.Mutex  //
	ConnList  []*RichConn //
	m_plogger *logrus.Entry
}

func (pool *ConnPool) Init(pSize int, servlURL string) {
	pool.cond = sync.NewCond(&sync.Mutex{})
	pool.MaxPool = pSize
	pool.ConnURL = servlURL
	if pool.m_plogger == nil {
		pool.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	go pool.goKeepConnPool()
	//go pool.goDelBadConn()
}

func (pool *ConnPool) goKeepConnPool() {
	/*
		维持链接池数量
	*/
	pool.m_plogger.Info("Start Keep Rabbit Connection Pool. ")
	for {
		if len(pool.ConnList) < pool.MaxPool {
			pool.poolLock.Lock()
			serv := RabbServer{URL: pool.ConnURL}
			err := serv.InitConnection()
			if err != nil {
				pool.m_plogger.Errorf("Keep Connection Pool error. ", err)
			} else {
				var Conn RichConn
				Conn.Serv = &serv
				pool.ConnList = append(pool.ConnList, &Conn)
			}
			pool.poolLock.Unlock()
			time.Sleep(time.Millisecond * 30)
			pool.m_plogger.Infof("Current RabbitMQ Connection Pool size: ", len(pool.ConnList))
		}
		if len(pool.ConnList) > 0 {
			pool.cond.Signal()
		}
		time.Sleep(time.Millisecond * 30)
	}
}

func (pool *ConnPool) goDelBadConn() {
	/*
		删除无效的链接, 断开的链接正常情况下会主动重连, 防止万一，进行删除处理
	*/
	fmt.Println("Start Remove Rabbit Connection Pool. ")
	pool.cond.L.Lock()
	if len(pool.ConnList) == 0 {
		pool.cond.Wait()
	}
	pool.cond.L.Unlock()
	for {
		for index, conn := range pool.ConnList {
			if !conn.Serv.ConnOk {
				fmt.Println(index)
			}
		}
		time.Sleep(time.Millisecond * 30)
	}
}

func (pool *ConnPool) GetConnect() (conn *RichConn, err error) {
	defer func() {
		if err1 := recover(); err1 != nil {
			pool.m_plogger.Error(err1)
			err = errors.New("Panic in GetConnect")
		}
	}()
	pool.cond.L.Lock()
	defer pool.cond.L.Unlock()
	if len(pool.ConnList) == 0 {
		err = errors.New("No Connections here. ")
		pool.cond.Wait()
	}
	maxRetry := 0 // 设置最大尝试次数， MQ或网络无异常时一般不会用上
	for {
		index := rand.Intn(len(pool.ConnList))
		if pool.ConnList[index].Serv.ConnOk {
			conn = pool.ConnList[index]
			err = nil
			return
		} else {
			if maxRetry >= len(pool.ConnList) {
				err = errors.New("Can't get Ok Rabbit Connection. ")
				return
			}
			maxRetry++
		}
	}
}

func (pool *ConnPool) SendWithQueueJson(q_name string, ex bool, dur bool, data []byte) error {
	conn, err0 := pool.GetConnect()
	if err0 != nil {
		return err0
	} else {
		queue, err := conn.Serv.GetOrCreateQueue(q_name, ex, dur)
		if err != nil {
			return err
		} else {
			if err2 := conn.Serv.PublishMessageJson(conn.Serv.Channel, queue.QueueObj, data); err2 != nil {
				return err2
			} else {
				return nil
			}
		}
	}
}

func (pool *ConnPool) SendWithQueue(q_name string, ex bool, dur bool, data []byte, isJson bool) error {
	var err2 error
	conn, err0 := pool.GetConnect()
	if err0 != nil || conn == nil {
		return err0
	} else {
		queue, err := conn.Serv.GetOrCreateQueue(q_name, ex, dur)
		if err != nil {
			return err
		} else {
			if isJson {
				err2 = conn.Serv.PublishMessageJson(conn.Serv.Channel, queue.QueueObj, data)
			} else {
				err2 = conn.Serv.PublishMessage(conn.Serv.Channel, queue.QueueObj, data)
			}
			return err2
		}
	}
}

func (pool *ConnPool) SendWithExchangeTopic(exName, route, Kind string, dur bool, isJson bool, data []byte) error {
	var err2 error
	pool.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{
		"Exchange": exName,
		"Routing":  route,
	})
	conn, err0 := pool.GetConnect()
	if err0 != nil || conn == nil {
		pool.m_plogger.Errorf("GetConnect Error: %v ", err0)
		return err0
	} else {
		if err := conn.Serv.GetOrCreateExchange(exName, Kind, dur, false, false, false); err != nil {
			pool.m_plogger.Errorf("Send To GetOrCreateExchange Error: %v", err)
		} else {
			if isJson {
				if err2 = conn.Serv.PublishMessageTopic(conn.Serv.Channel, exName, route, MIME_Json, data); err2 != nil {
					pool.m_plogger.Errorf("PublishMessageTopic Error: %v ", err2)
				} else {
					pool.m_plogger.Info("PublishMessageTopic Success")
				}
			} else {
			}
		}
		return err2
	}
}
