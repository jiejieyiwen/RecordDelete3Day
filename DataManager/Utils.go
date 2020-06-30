package DataManager

import (
	"RecordDelete3Day/Redis"
	"fmt"
	"github.com/sirupsen/logrus"
	"iPublic/LoggerModular"
	"sync/atomic"
)

var DeviceCount int32

var CurDay int32 = 7

func (pThis *DataManager) GetAllStorageDays() []StorageDaysInfo {
	rec := Redis.GetRedisRecordManager()
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	var Result []StorageDaysInfo
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{})
	for _, chStorageInfo := range pThis.SliceChannelStorageInfo {
		schemeInfo, err := rec.GetStorageSchemeInfoByStorageSchemeID(chStorageInfo.StorageSchemeId)
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
		mountpoint, err := rec.GetMountPointFromRedis(chStorageInfo.StorageMediumId)
		if err != nil {
			pThis.logger.Errorf("Get MountPoint From Redis Error: ChannelID: [%v], error: [%v]", chStorageInfo.DeviceId, err)
			continue
		}
		mountpoint += "/"
		Result = append(Result, StorageDaysInfo{ChannelInfo: chStorageInfo.DeviceId, StorageDays: schemeInfo.StorageDays, Path: mountpoint, Type: schemeInfo.StorageSchemeInfoType})
		atomic.AddInt32(&DeviceCount, 1)
		logger.Infof("Get Channel [%s] Storage Days [%d], MountPonit [%v], Type [%v] ok ", chStorageInfo.DeviceId, schemeInfo.StorageDays, mountpoint, schemeInfo.StorageSchemeInfoType)
	}
	return Result
}
