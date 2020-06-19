package Redis

import (
	"Config"
	"RecordDelete3Day/DataDefine"
	"encoding/json"
	"github.com/sirupsen/logrus"
	ipublic "iPublic/DataFactory/DataDefine/ProtoBuf"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"strings"
	"sync"
	"time"
)

type RecordFileRedis struct {
	Srv      *RedisModular.RedisConn // RedisConnect
	Redisurl string                  //redis地址
	Logger   *logrus.Entry
}

var recordManager RecordFileRedis

func GetRedisRecordManager() *RecordFileRedis {
	return &recordManager
}

//type ClusterRedis struct {
//	Srv    *RedisModular.RedisClustorConn
//	Logger *logrus.Entry
//}
//
//var clusterRedisCon ClusterRedis
//
//func GetClusterRedisCon() *ClusterRedis {
//	return &clusterRedisCon
//}

func init() {
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	//clusterRedisCon.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func Init() error {
	logger := LoggerModular.GetLogger()
	recordManager.Srv = RedisModular.GetRedisPool()
	recordManager.Redisurl = Config.GetConfig().PublicConfig.RedisURL

	//recordManager.Redisurl = "redis://:inphase123.@127.0.0.1:15675/2"
	//recordManager.Redisurl = "redis://:inphase123.@192.168.2.64:23680/2"

	err := recordManager.Srv.DaliWithURL(recordManager.Redisurl)
	if err != nil {
		logger.Errorf("Init Redis Failed, addr [%v], Error: [%v]", recordManager.Redisurl, err.Error())
		return err
	} else {
		logger.Infof("Init Redis Success: [%v]", recordManager.Redisurl)
		return nil
	}

	//clusterRedisCon.Srv = RedisModular.GetClustRedisPool()
	//clusterRedisCon.Srv.Addr = append(clusterRedisCon.Srv.Addr, "192.168.2.64:20301")
	//clusterRedisCon.Srv.Addr = append(clusterRedisCon.Srv.Addr, "192.168.2.64:20302")
	//clusterRedisCon.Srv.Addr = append(clusterRedisCon.Srv.Addr, "192.168.2.64:20303")
	//clusterRedisCon.Srv.Addr = append(clusterRedisCon.Srv.Addr, "192.168.2.64:20304")
	//clusterRedisCon.Srv.PoolSize = 10
	//clusterRedisCon.Srv.Password = "Wow&Y#CanReally4"
	//
	//if err := clusterRedisCon.Srv.DialWithUrl(); err != nil {
	//	logger.Errorf("ClusterCon err: [%v]", err.Error())
	//	return err
	//} else {
	//	logger.Infof("Init Cluster Redis Success: [%v]", clusterRedisCon.Srv.Addr)
	//	return nil
	//}
}

func (record *RecordFileRedis) GetStorageSchemeInfoFromRedis(info *[]ipublic.StorageSchemeInfo, key string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	key = "DC_StorageSchemeInfo:Data"
	pStringStringMapCmd := record.Srv.Client.HGetAll(key)
	data, err := pStringStringMapCmd.Result()
	if err != nil {
		record.Logger.Errorf("read StorageSchemeInfo error: %v\n", err)
		return err
	} else {
		record.Logger.Infof("read StorageSchemeInfo successs")
		var ChannelList ipublic.StorageSchemeInfo
		for _, value := range data {
			err = json.Unmarshal([]byte(value), &ChannelList)
			if err != nil {
				record.Logger.Errorf(" unmarshal StorageSchemeInfo failed!~~")
				return err
			} else {
				*info = append(*info, ChannelList)
			}
		}
	}
	return nil
}

func (record *RecordFileRedis) GetChannelStorageInfoFromRedis(info *[]ipublic.ChannelStorageInfo, key string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	key = "DC_ChannelStorageInfo:Data"
	pStringStringMapCmd := record.Srv.Client.HGetAll(key)
	if pStringStringMapCmd == nil {
		record.Logger.Infof("No Key Found~~!")
		return
	}
	data, err := pStringStringMapCmd.Result()
	if err != nil {
		record.Logger.Infof("Get ChannelStorageInfo from redis error: %v\n", err)
		count := 0
		for {
			pStringStringMapCmd := record.Srv.Client.HGetAll(key)
			if pStringStringMapCmd == nil {
				record.Logger.Infof("No Key Found~~!")
				return
			}
			data, err := pStringStringMapCmd.Result()
			if err != nil {
				count++
				if count >= 3 {
					break
				}
				time.Sleep(time.Second * 3)
				continue
			} else {
				record.Logger.Infof("Get all ChannelStorageInfo from redis Success")
				var ChannelList ipublic.ChannelStorageInfo
				for _, value := range data {
					err = json.Unmarshal([]byte(value), &ChannelList)
					if err != nil {
						record.Logger.Infof(" unmarshal ChannelStorageInfo failed,err:%v", err.Error())
						return err
					} else {
						*info = append(*info, ChannelList)
					}
				}
				break
			}
		}
		return err
	} else {
		//ignore
		record.Logger.Infof("Get all ChannelStorageInfo from redis Success")
		var ChannelList ipublic.ChannelStorageInfo
		for _, value := range data {
			err = json.Unmarshal([]byte(value), &ChannelList)
			if err != nil {
				record.Logger.Infof(" unmarshal ChannelStorageInfo failed,err:%v", err.Error())
				return err
			} else {
				*info = append(*info, ChannelList)
			}
		}
	}
	return err
}

func (record *RecordFileRedis) GetChannelStorageInfoFromRedisNew(info *[]ipublic.StorageData, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	keys := record.Srv.Client.Keys("DC_StorageChannelInfo_*")
	for _, v := range keys.Val() {
		pStringStringMapCmd := record.Srv.Client.HGetAll(v)
		if pStringStringMapCmd == nil {
			record.Logger.Infof("No Key Found~~!")
			continue
		}
		data, err := pStringStringMapCmd.Result()
		if err != nil {
			record.Logger.Infof("Get ChannelStorageInfo from redis error: %v\n", err)
			for {
				pStringStringMapCmd := record.Srv.Client.HGetAll(v)
				data, err1 := pStringStringMapCmd.Result()
				if err1 != nil {
					record.Logger.Infof("ReGet ChannelStorageInfo from redis error: %v\n", err1)
					time.Sleep(time.Second * 3)
					continue
				} else {
					record.Logger.Infof("ReGet all ChannelStorageInfo from redis Success: [%v]", len(data))
					var ChannelList ipublic.StorageData
					for _, value := range data {
						err = json.Unmarshal([]byte(value), &ChannelList)
						if err != nil {
							record.Logger.Infof("ReUnmarshal ChannelStorageInfo failed,err:%v", err.Error())
							continue
						} else {
							*info = append(*info, ChannelList)
						}
					}
					break
				}
			}
		} else {
			record.Logger.Infof("Get all ChannelStorageInfo from redis Success: [%v]", len(data))
			var ChannelList ipublic.StorageData
			for _, value := range data {
				err = json.Unmarshal([]byte(value), &ChannelList)
				if err != nil {
					record.Logger.Infof("Unmarshal ChannelStorageInfo failed,err:%v", err.Error())
					continue
				} else {
					*info = append(*info, ChannelList)
				}
			}
		}
	}
	return err
}

func (record *RecordFileRedis) GetDeleteServerConfig() (pSeverInfo map[string]string) {
	pSeverInfo = make(map[string]string)
	pStringSliceCmd := record.Srv.Client.Keys(DataDefine.KEY_NAME_SERVER_CONFIG)
	if nil != pStringSliceCmd.Err() {
		record.Logger.Error(pStringSliceCmd.Err().Error())
		return
	}
	for _, key := range pStringSliceCmd.Val() {
		strAllMountPoint, err := record.Srv.Get(key)
		if nil != err {
			continue
		}
		//地址
		arrStr := strings.Split(key, ":")
		strServerAddr := arrStr[1]
		keys := DataDefine.HOST_SERVER_CONFIG
		keys += strServerAddr
		pStringSliceCmd1 := record.Srv.Client.Get(keys)
		if nil != pStringSliceCmd1.Err() {
			record.Logger.Error(pStringSliceCmd1.Err().Error())
			return
		}
		//挂载点
		pSeverInfo[pStringSliceCmd1.Val()] = strAllMountPoint
	}
	return
}

func (record *RecordFileRedis) GetMountPointFromRedis(key string) (string, error) {
	filed := "DC_StorageMediumInfo:Data"
	data, err := record.Srv.Client.HGet(filed, key).Result()
	if err != nil {
		return "", err
	}
	var datalist ipublic.StorageMediumInfo
	err = json.Unmarshal([]byte(data), &datalist)
	if err != nil {
		return "", err
	}
	return datalist.StorageMediumInfoPath, nil
}

func (record *RecordFileRedis) GetSeverList(key string) (err error, add string) {
	pStringSliceCmd := record.Srv.Client.Get(DataDefine.HOST_SERVER_CONFIG + key)
	if nil != pStringSliceCmd.Err() {
		record.Logger.Error(pStringSliceCmd.Err().Error())
		return pStringSliceCmd.Err(), ""
	}
	return nil, pStringSliceCmd.Val()
}

//func (record *RecordFileRedis) WriteDeviceNum(device DeviceNum) error {
//	date := time.Now().Format("2006-01-02")
//	key := "DeviceNum_" + date
//	data, err := json.Marshal(device)
//	if err != nil {
//		return err
//	}
//	return record.Srv.Client.Set(key, data, time.Hour*72).Err()
//}
