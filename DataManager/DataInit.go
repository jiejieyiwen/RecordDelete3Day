package DataManager

import (
	SDataDefine "StorageMaintainer1/DataDefine"
	"StorageMaintainer1/Redis"
	"iPublic/DataFactory/DataDefine"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/iAuthorize"
	"sync"
	"time"
)

var Size int64

func (pThis *DataManager) Init() error {
	pThis.logger = LoggerModular.GetLogger()
	pThis.bRunning = true
	pThis.bRecicvedGRPCNotify = false
	pThis.TaskMap = []StorageDaysInfo{}
	pThis.GetNewChannelStorage()
	pThis.NeedDeleteTsList1 = make(chan SDataDefine.RecordFileInfo, 100)
	return nil
}

func (pThis *DataManager) flushData() {
	pThis.SliceChannelStorageInfo = []DataDefine.ChannelStorageInfo{}
}

func (pThis *DataManager) goRefreshPlatformToken() {
	conf := EnvLoad.GetConf()
	for pThis.bRunning {
		strToken, err := iAuthorize.AesAuthorize(conf.Url.AuthURL, conf.AuthConfig.DataUserName, conf.AuthConfig.DataPassWord, conf.AuthConfig.ClientID, conf.AuthConfig.ClientSecret)
		if err != nil {
			pThis.logger.Error(err.Error())
		} else {
			pThis.PlatformToken = strToken
		}
		time.Sleep(5 * time.Minute)
	}
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
	lens, err := rec.Srv.Client.HLen("DC_StorageMediumInfo:Data").Result()
	if err != nil {
		pThis.logger.Errorf("Get DC_StorageMediumInfo:Data Len Failed:[%v]", err)
		return
	}
	Size = lens
	pThis.MapNeedDeleteList = make(map[string][]SDataDefine.RecordFileInfo, Size)

	StorageDaysInfos := pThis.GetAllStorageDays()

	//建立挂载点对列表
	pThis.MountPointList = make(map[string][]StorageDaysInfo, lens)
	var tempkey = make(map[string]string, lens)

	for index, stoDay := range StorageDaysInfos {
		if index == 0 {
			pThis.TaskMapLock.Lock()
			pThis.TaskMap = append(pThis.TaskMap, stoDay)
			pThis.TaskMapLock.Unlock()
			pThis.MountPointList[stoDay.Path] = append(pThis.MountPointList[stoDay.Path], stoDay)
			tempkey[stoDay.Path] = stoDay.Path
			continue
		}
		pThis.TaskMapLock.Lock()
		pThis.TaskMap = append(pThis.TaskMap, stoDay)
		pThis.TaskMapLock.Unlock()
		if _, ok := pThis.MountPointList[stoDay.Path]; !ok {
			pThis.MountPointList[stoDay.Path] = append(pThis.MountPointList[stoDay.Path], stoDay)
			tempkey[stoDay.Path] = stoDay.Path
		} else {
			key := tempkey[stoDay.Path]
			pThis.MountPointList[key] = append(pThis.MountPointList[key], stoDay)
		}
	}
	if len(pThis.TaskMap) != 0 {
		pThis.logger.Infof("Success to Get All Devices' StorageDay~! [%v]", len(pThis.TaskMap))
	}
	tempkey = make(map[string]string)
	//fmt.Println(pThis.TaskMap)
}

func (pThis *DataManager) GetMountPointMap() map[string][]StorageDaysInfo {
	pThis.MountPointListLock.Lock()
	defer pThis.MountPointListLock.Unlock()
	a := pThis.MountPointList
	pThis.MountPointList = make(map[string][]StorageDaysInfo, 0)
	return a
}

//获取固定长度需要删除的文件信息
func (pThis *DataManager) GetNeedDeleteTs(countLimit int) []SDataDefine.RecordFileInfo {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	if len(pThis.NeedDeleteTsList) == 0 {
		return pThis.NeedDeleteTsList
	}
	if len(pThis.NeedDeleteTsList) < countLimit {
		a := pThis.NeedDeleteTsList
		pThis.NeedDeleteTsList = []SDataDefine.RecordFileInfo{}
		return a
	} else {
		a := pThis.NeedDeleteTsList[:countLimit]
		pThis.NeedDeleteTsList = pThis.NeedDeleteTsList[countLimit:]
		return a
	}
}

func (pThis *DataManager) GetNeedDeleteTsch(countLimit int) []SDataDefine.RecordFileInfo {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	if len(pThis.NeedDeleteTsList) == 0 {
		return pThis.NeedDeleteTsList
	}
	if len(pThis.NeedDeleteTsList) < countLimit {
		a := pThis.NeedDeleteTsList
		pThis.NeedDeleteTsList = []SDataDefine.RecordFileInfo{}
		return a
	} else {
		a := pThis.NeedDeleteTsList[:countLimit]
		pThis.NeedDeleteTsList = pThis.NeedDeleteTsList[countLimit:]
		return a
	}
}

func (pThis *DataManager) GetAllNeedDeleteTs() map[string][]SDataDefine.RecordFileInfo {
	pThis.MapNeedDeleteListLock.Lock()
	defer pThis.MapNeedDeleteListLock.Unlock()
	if len(pThis.MapNeedDeleteList) == 0 {
		return pThis.MapNeedDeleteList
	}
	a := pThis.MapNeedDeleteList
	pThis.MapNeedDeleteList = make(map[string][]SDataDefine.RecordFileInfo)
	return a
}

//将查询到的需要删除的TS文件信息推入
func (pThis *DataManager) PushNeedDeleteTs(ts SDataDefine.RecordFileInfo) {
	pThis.SliceChannelStorageInfoLock.Lock()
	defer pThis.SliceChannelStorageInfoLock.Unlock()
	pThis.NeedDeleteTsList = append(pThis.NeedDeleteTsList, ts)
}

func (pThis *DataManager) PushNeedDeleteTsch(ts SDataDefine.RecordFileInfo) {
	pThis.NeedDeleteTsList1Lock.Lock()
	defer pThis.NeedDeleteTsList1Lock.Unlock()
	pThis.NeedDeleteTsList1 <- ts
}

func (pThis *DataManager) PushNeedDeleteTsByMountPoints(mountpoint string, ts SDataDefine.RecordFileInfo) {
	pThis.MapNeedDeleteListLock.Lock()
	defer pThis.MapNeedDeleteListLock.Unlock()
	pThis.MapNeedDeleteList[mountpoint] = append(pThis.MapNeedDeleteList[mountpoint], ts)
}

func (pThis *DataManager) GetNotifyStatus() bool {
	pThis.bRecicvedGRPCNotifyLock.Lock()
	defer pThis.bRecicvedGRPCNotifyLock.Unlock()
	return pThis.bRecicvedGRPCNotify
}

func (pThis *DataManager) SetNotifyStatus(istrue bool) {
	pThis.bRecicvedGRPCNotifyLock.Lock()
	defer pThis.bRecicvedGRPCNotifyLock.Unlock()
	pThis.bRecicvedGRPCNotify = istrue
}
