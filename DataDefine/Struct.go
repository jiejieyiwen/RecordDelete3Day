package DataDefine

type ChannelTaskEntity struct {
	DeviceID              string                    `json:"DeviceID"`
	SrcURL                string                    `json:"SrcURL"`
	StorageMediumInfoID   string                    `json:"StorageMediumInfo"`
	StoragePolicyInfoID   string                    `json:"StoragePolicyInfo"`
	StorageSchemeInfoType int                       `json:"StorageSchemeInfoType"`
	List                  []StorageSchemeDetailInfo `json:"StorageSchemeDetailInfo"`
}

type StorageSchemeDetailInfo struct {
	StorageSchemeDetailInfoID string `json:"StorageSchemeDetailInfoID"`
	StorageSchemeID           string `json:"StorageSchemeID"`
	WeekNum                   int    `json:"WeekNum"`
	StartDateTime             int    `json:"StartDateTime"`
	EndDataTime               int    `json:"EndDataTime"`
}
