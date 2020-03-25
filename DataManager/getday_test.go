package DataManager

import (
	"StorageMaintainer1/Redis"
	"fmt"
	"iPublic/DataFactory/DataDefine"
	"sync"
	"testing"
)

func TestGetdays(t *testing.T) {
	if err := Redis.Init(); err != nil {
		fmt.Println(err)
		return
	}
	var SliceChannelStorageInfo []DataDefine.ChannelStorageInfo
	rec := Redis.GetRedisRecordManager()
	var wg sync.WaitGroup
	wg.Add(1)
	rec.GetChannelStorageInfoFromRedis(&SliceChannelStorageInfo, "", &wg)
	wg.Wait()
	count := 0
	var ChannelStorageInfo []DataDefine.ChannelStorageInfo
	for _, v := range SliceChannelStorageInfo {
		if v.ChannelStorageInfoID == "1195646077690413057" {
			count++
			ChannelStorageInfo = append(ChannelStorageInfo, v)
			fmt.Println(v)
		}
	}
	fmt.Println(count)
	StorageDaysInfo := GetDataManager().GetAllStorageDaysbyID(ChannelStorageInfo)
	for _, v := range StorageDaysInfo {
		fmt.Println(v)
	}
	a := make(chan bool)
	<-a
}
