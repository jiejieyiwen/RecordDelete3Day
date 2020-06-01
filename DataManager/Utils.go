package DataManager

import (
	"RecordDelete3Day/Redis"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/sirupsen/logrus"
	DataDefine "iPublic/DataFactory/DataDefine/ProtoBuf"
	"iPublic/LoggerModular"
	"math"
	"net/http"
	"sync/atomic"
	"time"
)

const OneHour = 60 * 60
const IgnoreDays = 1

var DeviceCount int32

var CurDay int32

func (pThis *DataManager) GetAllStorageDays() []StorageDaysInfo {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	var Result []StorageDaysInfo
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{})
	for _, chStorageInfo := range pThis.SliceChannelStorageInfo {
		schemeInfo, err := GetDataManager().GetStorageSchemeInfoByStorageSchemeID(chStorageInfo.StorageSchemeId)
		if err != nil {
			ErrMsg := fmt.Sprintf("Can't find Channel StorageSchemeInfo For Channel ID:[%s], StorageSchemeID:[%s] ",
				chStorageInfo.DeviceId, chStorageInfo.StorageSchemeId)
			logger.Error(ErrMsg)
			continue
		}
		if schemeInfo.StorageDays != CurDay {
			continue
		}
		//获取设备挂载点
		rec := Redis.GetRedisRecordManager()
		mountpoint, err := rec.GetMountPointFromRedis(chStorageInfo.StorageMediumId)
		//if !strings.Contains(mountpoint, "yyxs") {
		//	continue
		//}
		if err != nil {
			pThis.logger.Errorf("Get MountPoint From Redis Error: ChannelID: [%v], error: [%v]", chStorageInfo.DeviceId, err)
			continue
		}
		mountpoint += "/"
		Result = append(Result, StorageDaysInfo{ChannelInfo: chStorageInfo.DeviceId, StorageDays: schemeInfo.StorageDays, Path: mountpoint})
		atomic.AddInt32(&DeviceCount, 1)
		logger.Infof("Get Channel [%s] Storage Days [%d], MountPonit [%v] ok ", chStorageInfo.DeviceId, schemeInfo.StorageDays, mountpoint)
		time.Sleep(time.Nanosecond)
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
