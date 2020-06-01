package DataManager

import (
	"RecordDelete3Day/Redis"
	DataDefine "iPublic/DataFactory/DataDefine/ProtoBuf"
	"iPublic/LoggerModular"
	"sync"
)

var Size int

func (pThis *DataManager) Init() error {
	pThis.logger = LoggerModular.GetLogger()
	pThis.GetNewChannelStorage()
	return nil
}

func (pThis *DataManager) flushData() {
	pThis.SliceChannelStorageInfo = []DataDefine.StorageData{}
}

//刷新通道等数据信息
func (pThis *DataManager) GetNewChannelStorage() {
	//先从redis里面获取所有的channel storage
	pThis.flushData()
	rec := Redis.GetRedisRecordManager()
	var wg sync.WaitGroup
	wg.Add(1)
	go rec.GetChannelStorageInfoFromRedisNew(&pThis.SliceChannelStorageInfo, &wg)
	wg.Wait()
	pThis.logger.Infof("Success to Get All ChannelStorageInfo: [%v]", len(pThis.SliceChannelStorageInfo))
	//获取所有设备的存储天数
	StorageDaysInfos := pThis.GetAllStorageDays()

	//建立挂载点对列表
	pThis.MountPointList = make(map[string][]StorageDaysInfo)
	var tempkey = make(map[string]string)

	for index, stoDay := range StorageDaysInfos {
		if index == 0 {
			pThis.MountPointList[stoDay.Path] = append(pThis.MountPointList[stoDay.Path], stoDay)
			tempkey[stoDay.Path] = stoDay.Path
			continue
		}
		if _, ok := pThis.MountPointList[stoDay.Path]; !ok {
			pThis.MountPointList[stoDay.Path] = append(pThis.MountPointList[stoDay.Path], stoDay)
			tempkey[stoDay.Path] = stoDay.Path
		} else {
			key := tempkey[stoDay.Path]
			pThis.MountPointList[key] = append(pThis.MountPointList[key], stoDay)
		}
	}

	if len(pThis.MountPointList) != 0 {
		pThis.logger.Infof("Success to Get All Devices' StorageDay~! [%v]", len(pThis.MountPointList))
		Size = len(pThis.MountPointList)
	}
}

func (pThis *DataManager) GetMountPointMap() map[string][]StorageDaysInfo {
	pThis.MountPointListLock.Lock()
	defer pThis.MountPointListLock.Unlock()
	a := pThis.MountPointList
	pThis.MountPointList = make(map[string][]StorageDaysInfo)
	return a
}
