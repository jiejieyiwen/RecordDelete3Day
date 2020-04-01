package MqModular

import (
	DataCenterDefine "DataCenterModular/DataDefine"
	"encoding/json"
	"errors"
	"github.com/sirupsen/logrus"
	"iPublic/LoggerModular"
	"io/ioutil"
	"net/http"
)

var pLogger *logrus.Entry

func init() {
	if pLogger == nil {
		pLogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
}
func GetServerURL(HTTP_URL string) (url string, size int, err error) {
	httpServerURL := HTTP_URL + "/ServConfig"
	resp, err := http.Get(httpServerURL)
	if err != nil {
		pLogger.Errorf("getServerURL Failed error : %v\n", err.Error())
		return "", 0, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var serverConfig DataCenterDefine.ServConfig
	err = json.Unmarshal(body, &serverConfig)
	if err != nil {
		pLogger.Errorf("getServerURL Unmarshal Failed,error :%v\n ", err.Error())
		return "", 0, err
	}
	propertySources, ok := serverConfig.PropertySources[0].(map[string]interface{})
	if ok {
		serverURL, ok := propertySources["source"].(map[string]interface{})
		if ok {
			url = serverURL["AMQPURL"].(string)
			pLogger.Info("Get AMQPURL OK ~")
			return url, 0, nil
		}
	}
	pLogger.Error("Get AMQPURL Find Failed")
	return "", 0, errors.New("Get AMQPURL Fail")
}
