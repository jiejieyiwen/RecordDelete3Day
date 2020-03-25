package StorageMaintainerGRpcClient

import (
	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"sync"
)

type GRpcClient struct {
	m_pClientCon *grpc.ClientConn
	m_sw         sync.WaitGroup
}

func (pThis *GRpcClient) GRpcDial(strIp string) error {

	//如果已经连接，则断开连接，重新连接
	pThis.Close()
	clientCon, err := grpc.Dial(strIp, grpc.WithInsecure())
	if err != nil {
		return err
	}
	//state := clientCon.GetState()
	//if state != connectivity.Connecting {
	//	return errors.New("Not Connecting")
	//}
	pThis.m_pClientCon = clientCon

	return nil
}

func (pThis *GRpcClient) Close() {
	if nil != pThis.m_pClientCon {
		pThis.m_pClientCon.Close()
		pThis.m_pClientCon = nil
	}
}

func (pThis *GRpcClient) Notify(strChannelID string, strRelativePath string, strMountPoint string, strDate string, strRecordID string, nStartTime int64) (*StorageMaintainerMessage.StreamResData, error) {
	if nil == pThis.m_pClientCon {
		return nil, errors.New("Client Has No Connected")
	}
	pThis.m_sw.Add(1)
	defer pThis.m_sw.Done()
	c := StorageMaintainerMessage.NewGreeterClient(pThis.m_pClientCon)
	req := StorageMaintainerMessage.StreamReqData{
		StrChannelID:    strChannelID,
		StrRelativePath: strRelativePath,
		StrMountPoint:   strMountPoint,
		StrDate:         strDate,
		StrRecordID:     strRecordID,
		NStartTime:      nStartTime}
	//调用服务端推送流
	res, _ := c.GetStream(context.Background(), &req)
	//循环接受推流
	for res != nil {
		pRespon, err := res.Recv()
		if err != nil {
			return nil, err
		}
		if pRespon.NRespond != 0 {
			return pRespon, nil
		}
	}
	return nil, errors.New("Connect Failed")
}
