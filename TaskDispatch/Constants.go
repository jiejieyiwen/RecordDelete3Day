package TaskDispatch

import (
	SDataDefine "StorageMaintainer1/DataDefine"
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerGRpcClient"
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"sync"
)

type ChannelDeleteTask struct {
	ChannelID  string // 通道信息
	BeforeTime int64  // 删除几天之前
}

type DeleteServer struct {
	m_pClient  *StorageMaintainerGRpcClient.GRpcClient
	m_chTask   chan SDataDefine.RecordFileInfo
	m_chResult chan StorageMaintainerMessage.StreamResData
}

type DeleteServer1 struct {
	m_pClient  *StorageMaintainerGRpcClient.GRpcClient
	mountpoint string
}

func NewDeleteServer(strAddr string, nTaskSize int) (*DeleteServer, error) {
	deleteServer := &DeleteServer{
		&StorageMaintainerGRpcClient.GRpcClient{},
		make(chan SDataDefine.RecordFileInfo, nTaskSize),
		make(chan StorageMaintainerMessage.StreamResData, nTaskSize),
	}
	err := deleteServer.m_pClient.GRpcDial(strAddr)
	return deleteServer, err
}

func NewDeleteServer1(strAddr string) (*DeleteServer1, error) {
	deleteServer := &DeleteServer1{
		&StorageMaintainerGRpcClient.GRpcClient{},
		strAddr,
	}
	err := deleteServer.m_pClient.GRpcDial(strAddr)
	return deleteServer, err
}

func CloseDeleteServer(pDeleteServer *DeleteServer) {
	close(pDeleteServer.m_chTask)
	close(pDeleteServer.m_chResult)
}

type DeleteServerInfo struct {
	Con        *StorageMaintainerGRpcClient.GRpcClient
	Mountponit string
	task       chan SDataDefine.RecordFileInfo
}

type DeleteTask struct {
	bRunning bool
	cLock    sync.Mutex
	logger   *logrus.Entry

	m_pDiskPercent *prometheus.GaugeVec

	M_wg sync.WaitGroup

	m_RevertId     []bson.ObjectId
	m_RevertIdLock sync.Mutex

	m_mapDeleteServerList     map[string]string
	m_mapDeleteServerListLock sync.Mutex

	DeleteServerList     map[string]*DeleteServerInfo
	DeleteServerListLock sync.Mutex

	m_chResults chan StorageMaintainerMessage.StreamResData
}

var dTask DeleteTask

func GetTaskManager() *DeleteTask {
	return &dTask
}

func (manager *DeleteTask) AddRevertId(id bson.ObjectId) {
	manager.m_RevertIdLock.Lock()
	defer manager.m_RevertIdLock.Unlock()
	manager.m_RevertId = append(manager.m_RevertId, id)
}

func (manager *DeleteTask) GetRevertId() []bson.ObjectId {
	manager.m_RevertIdLock.Lock()
	defer manager.m_RevertIdLock.Unlock()
	temp := manager.m_RevertId
	manager.m_RevertId = []bson.ObjectId{}
	return temp
}

func (manager *DeleteTask) GetDeleteServerList() map[string]string {
	manager.m_mapDeleteServerListLock.Lock()
	defer manager.m_mapDeleteServerListLock.Unlock()
	temp := manager.m_mapDeleteServerList
	return temp
}
