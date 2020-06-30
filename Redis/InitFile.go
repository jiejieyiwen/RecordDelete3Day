package Redis

import (
	"Config"
	"RecordDelete3Day/DataDefine"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	ipublic "iPublic/DataFactory/DataDefine/ProtoBuf"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net/url"
	"strings"
	"sync"
	"time"
)

type RecordFileRedis struct {
	Srv        *RedisModular.RedisConn // RedisConnect
	SrvCluster *RedisModular.RedisClustorConn
	Redisurl   string //redis地址
	Logger     *logrus.Entry
	Type       int
}

var recordManager RecordFileRedis

func GetRedisRecordManager() *RecordFileRedis {
	return &recordManager
}

func init() {
	recordManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func Init() error {
	logger := LoggerModular.GetLogger()
	recordManager.Srv = RedisModular.GetRedisPool()
	recordManager.Redisurl = Config.GetConfig().PublicConfig.RedisURL
	//recordManager.Redisurl = "redis://:R7OxmC3HYg@redis.mj.com:6379/0?PoolSize=5"

	if strings.Contains(recordManager.Redisurl, ",") {
		recordManager.Type = 1
		recordManager.SrvCluster = RedisModular.GetClustRedisPool()
		u, err := url.Parse(recordManager.Redisurl)
		if err != nil {
			panic(err)
		}
		add := strings.Split(u.Host, ",")
		for _, v := range add {
			recordManager.SrvCluster.Addr = append(recordManager.SrvCluster.Addr, v)
		}
		recordManager.SrvCluster.PoolSize = 5
		p, _ := u.User.Password()
		recordManager.SrvCluster.Password = p

		if err := recordManager.SrvCluster.DialWithUrl(); err != nil {
			logger.Errorf("ClusterCon err: [%v]", err.Error())
			return err
		} else {
			logger.Infof("Init Cluster Redis Success: [%v]", recordManager.SrvCluster.Addr)
			return nil
		}
	} else {
		recordManager.Type = 2
		err := recordManager.Srv.DaliWithURL(recordManager.Redisurl)
		if err != nil {
			logger.Errorf("Init Redis Failed, addr [%v], Error: [%v]", recordManager.Redisurl, err.Error())
			return err
		} else {
			logger.Infof("Init Redis Success: [%v]", recordManager.Redisurl)
			return nil
		}
	}
}

func (record *RecordFileRedis) GetChannelStorageInfoFromRedisNew(info *[]ipublic.StorageData, wg *sync.WaitGroup) (err error) {
	defer wg.Done()

	var keys *redis.StringSliceCmd

	//用不同的srv
	if record.Type == 1 {
		keys = record.SrvCluster.Client.Keys("DC_StorageChannelInfo_*")
	} else {
		keys = record.Srv.Client.Keys("DC_StorageChannelInfo_*")
	}

	var pStringStringMapCmd *redis.StringStringMapCmd
	for _, v := range keys.Val() {
		if record.Type == 1 {
			pStringStringMapCmd = record.SrvCluster.Client.HGetAll(v)
		} else {
			pStringStringMapCmd = record.Srv.Client.HGetAll(v)
		}

		if pStringStringMapCmd == nil {
			record.Logger.Infof("No Key Found~~!")
			continue
		}
		data, err := pStringStringMapCmd.Result()
		if err != nil {
			record.Logger.Infof("Get ChannelStorageInfo from redis error: %v\n", err)
			for {
				var pStringStringMapCmd *redis.StringStringMapCmd
				if record.Type == 1 {
					pStringStringMapCmd = record.SrvCluster.Client.HGetAll(v)
				} else {
					pStringStringMapCmd = record.Srv.Client.HGetAll(v)
				}
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
	var pStringSliceCmd *redis.StringSliceCmd
	if record.Type == 1 {
		pStringSliceCmd = record.SrvCluster.Client.Keys(DataDefine.KEY_NAME_SERVER_CONFIG)
	} else {
		pStringSliceCmd = record.Srv.Client.Keys(DataDefine.KEY_NAME_SERVER_CONFIG)
	}

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

		var pStringSliceCmd1 *redis.StringCmd
		if record.Type == 1 {
			pStringSliceCmd1 = record.SrvCluster.Client.Get(keys)
		} else {
			pStringSliceCmd1 = record.Srv.Client.Get(keys)
		}

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
	var StringCmd *redis.StringCmd
	if record.Type == 1 {
		StringCmd = record.SrvCluster.Client.HGet(filed, key)
	} else {
		StringCmd = record.Srv.Client.HGet(filed, key)
	}
	data, err := StringCmd.Result()
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
	var pStringSliceCmd *redis.StringCmd
	if record.Type == 1 {
		pStringSliceCmd = record.SrvCluster.Client.Get(DataDefine.HOST_SERVER_CONFIG + key)
	} else {
		pStringSliceCmd = record.Srv.Client.Get(DataDefine.HOST_SERVER_CONFIG + key)
	}
	if nil != pStringSliceCmd.Err() {
		record.Logger.Error(pStringSliceCmd.Err().Error())
		return pStringSliceCmd.Err(), ""
	}
	return nil, pStringSliceCmd.Val()
}

func (record *RecordFileRedis) GetStorageSchemeInfoByStorageSchemeID(strStorageSchemeID string) (ipublic.StorageSchemeInfo, error) {
	tReturn := ipublic.StorageSchemeInfo{}
	strPrefix := "DC_StorageSchemeInfo:Data"
	var pStringCmd *redis.StringCmd
	if record.Type == 1 {
		pStringCmd = record.SrvCluster.Client.HGet(strPrefix, strStorageSchemeID)
	} else {
		pStringCmd = record.Srv.Client.HGet(strPrefix, strStorageSchemeID)
	}
	if nil != pStringCmd.Err() {
		return tReturn, errors.New(fmt.Sprintf("Can`t Find StorageSchemeInfo:[%s] StorageSchemeID !", strStorageSchemeID))
	}
	strData, _ := pStringCmd.Result()
	json.Unmarshal([]byte(strData), &tReturn)
	return tReturn, nil
}
