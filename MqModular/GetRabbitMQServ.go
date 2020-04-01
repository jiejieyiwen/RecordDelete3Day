package MqModular

import "time"

func GetRabbitMQServ(Url string, serv *RabbServer) error {
	_serv := RabbServer{
		URL: Url,
	}
	err := _serv.InitConnection()
	if err != nil || !_serv.ConnOk {
		time.Sleep(time.Second * 3)
		return nil
	} else {
		*serv = _serv
		return nil
	}
}
