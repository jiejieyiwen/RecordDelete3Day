package DataManager

import (
	SDataDefine "RecordDelete3Day/DataDefine"
	"RecordDelete3Day/Redis"
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerMessage"
	DataDefine "iPublic/DataFactory/DataDefine/ProductPlatformDataDefine"
	"iPublic/LoggerModular"
	"sync"
)

var Size int

func (pThis *DataManager) Init() error {
	pThis.logger = LoggerModular.GetLogger()
	pThis.bRunning = true
	pThis.TaskMap = []StorageDaysInfo{}
	pThis.GetNewChannelStorage()
	return nil
}

func (pThis *DataManager) flushData() {
	pThis.SliceChannelStorageInfo = []DataDefine.ChannelStorageInfo{}
}

//刷新通道等数据信息
func (pThis *DataManager) GetNewChannelStorage() {
	//先从redis里面获取所有的channel storage
	pThis.flushData()
	rec := Redis.GetRedisRecordManager()
	var wg sync.WaitGroup
	wg.Add(1)
	go rec.GetChannelStorageInfoFromRedis(&pThis.SliceChannelStorageInfo, "", &wg)
	wg.Wait()

	//获取所有设备的存储天数

	pThis.MapNeedDeleteList = make(map[string][]SDataDefine.RecordFileInfo, Size)

	StorageDaysInfos := pThis.GetAllStorageDays()

	//建立挂载点对列表
	pThis.MountPointList = make(map[string][]StorageDaysInfo)
	pThis.MountPointMQList = make(map[string]chan StorageMaintainerMessage.StreamResData)
	var tempkey = make(map[string]string)

	for index, stoDay := range StorageDaysInfos {
		if index == 0 {
			pThis.MountPointList[stoDay.Path] = append(pThis.MountPointList[stoDay.Path], stoDay)
			pThis.MountPointMQList[stoDay.Path] = make(chan StorageMaintainerMessage.StreamResData, 1024)
			tempkey[stoDay.Path] = stoDay.Path
			continue
		}
		if _, ok := pThis.MountPointList[stoDay.Path]; !ok {
			pThis.MountPointList[stoDay.Path] = append(pThis.MountPointList[stoDay.Path], stoDay)
			pThis.MountPointMQList[stoDay.Path] = make(chan StorageMaintainerMessage.StreamResData, 1024)
			tempkey[stoDay.Path] = stoDay.Path
		} else {
			key := tempkey[stoDay.Path]
			pThis.MountPointList[key] = append(pThis.MountPointList[key], stoDay)
			pThis.MountPointMQList[stoDay.Path] = make(chan StorageMaintainerMessage.StreamResData, 1024)
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
	pThis.MountPointList = make(map[string][]StorageDaysInfo, 0)
	return a
}

func (pThis *DataManager) GetMountPointMQMap() map[string]chan StorageMaintainerMessage.StreamResData {
	pThis.MountPointMQListLock.Lock()
	defer pThis.MountPointMQListLock.Unlock()
	return pThis.MountPointMQList
}

func (pThis *DataManager) GetNeedDeleteTsAll() []SDataDefine.RecordFileInfo {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	if len(pThis.NeedDeleteTsList) == 0 {
		return pThis.NeedDeleteTsList
	}
	a := pThis.NeedDeleteTsList
	pThis.NeedDeleteTsList = []SDataDefine.RecordFileInfo{}
	return a

}

//将查询到的需要删除的TS文件信息推入
func (pThis *DataManager) PushNeedDeleteTs(ts SDataDefine.RecordFileInfo) {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	pThis.NeedDeleteTsList = append(pThis.NeedDeleteTsList, ts)
}
