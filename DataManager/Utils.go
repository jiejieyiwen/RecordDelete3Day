package DataManager

import (
	"RecordDelete3Day/Redis"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/sirupsen/logrus"
	DataDefine "iPublic/DataFactory/DataDefine/ProductPlatformDataDefine"
	"iPublic/LoggerModular"
	"math"
	"net/http"
	"time"
)

const OneHour = 60 * 60
const IgnoreDays = 1

func (pThis *DataManager) GetAllStorageDays() []StorageDaysInfo {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	var Result []StorageDaysInfo
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{})
	for _, chStorageInfo := range pThis.SliceChannelStorageInfo {
		schemeInfo, err := GetDataManager().GetStorageSchemeInfoByStorageSchemeID(chStorageInfo.StorageSchemeID)
		if err != nil {
			ErrMsg := fmt.Sprintf("Can't find Channel StorageSchemeInfo For Channel ID:[%s], StorageSchemeID:[%s] ",
				chStorageInfo.ChannelID, chStorageInfo.StorageSchemeID)
			logger.Error(ErrMsg)
			continue
		}
		//if schemeInfo.StorageDays != 3 {
		//	continue
		//}
		//获取设备挂载点
		rec := Redis.GetRedisRecordManager()
		mountpoint, err := rec.GetMountPointFromRedis(chStorageInfo.StorageMediumID)
		if err != nil {
			pThis.logger.Errorf("Get MountPoint From Redis Error: ChannelID: [%v], error: [%v]", chStorageInfo.ChannelID, err)
			continue
		}
		Result = append(Result, StorageDaysInfo{ChannelInfo: chStorageInfo.ChannelID, StorageDays: schemeInfo.StorageDays, Path: mountpoint})
		logger.Infof("Get Channel [%s] Storage Days [%d], MountPonit [%v] ok ", chStorageInfo.ChannelID, schemeInfo.StorageDays, mountpoint)
	}
	return Result
}

func (pThis *DataManager) GetStorageSchemeInfoByStorageSchemeID(strStorageSchemeID string) (DataDefine.StorageSchemeInfo, error) {
	tReturn := DataDefine.StorageSchemeInfo{}
	rec := Redis.GetRedisRecordManager()
	strPrefix := "DC_StorageSchemeInfo:Data"
	pStringCmd := rec.Srv.Client.HGet(strPrefix, strStorageSchemeID)
	if nil != pStringCmd.Err() {
		return tReturn, errors.New(fmt.Sprintf("Can`t Find StorageSchemeInfo:[%s] StorageSchemeID !", strStorageSchemeID))
	}
	strData, _ := pStringCmd.Result()
	json.Unmarshal([]byte(strData), &tReturn)
	return tReturn, nil
}

func GetSubDayMorningTimeStamp(subDay int) (int64, error) {
	tNow := time.Now()
	mainTime := time.Date(tNow.Year(), tNow.Month(), tNow.Day(), 0, 0, 0, 0, time.UTC) // TODO 需确认之前的时区
	DaysAgo := mainTime.AddDate(0, 0, -(subDay + IgnoreDays))                          // 多保留一天的量
	TimeStamp := DaysAgo.Unix()
	return TimeStamp - 8*OneHour, nil
}

func CheckNetworkTimeWithNTSC() bool {
	var client http.Client
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{"NTSC": NTSC_URL})
	if req, err := http.NewRequest("GET", NTSC_URL, nil); err != nil {
		logger.Errorf("Get Network Time Error: [%v]", err)
		return false
	} else {
		if resp, err := client.Do(req); err != nil {
			logger.Errorf("Request Network Time Error: [%v]", err)
			return false
		} else {
			defer resp.Body.Close()
			strDay := resp.Header.Get("Date")
			if t, err := dateparse.ParseLocal(strDay); err != nil { // 得出时间加8小时
				logger.Errorf("Parse Web Date Error: [%v]", err)
				return false
			} else {
				tNow := time.Now()
				remoteTs := t.Unix() + 8*OneHour
				localTS := tNow.Unix()
				subTS := localTS - remoteTs
				if math.Abs(float64(subTS)) > OneHour {
					//logger.Errorf("Local - NTP = [%s]", subTS)
					return false
				} else {
					return true
				}
			}
		}
	}
}
