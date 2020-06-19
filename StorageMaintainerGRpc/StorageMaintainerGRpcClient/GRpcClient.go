package StorageMaintainerGRpcClient

import (
	"RecordDelete3Day/StorageMaintainerGRpc/StorageMaintainerMessage"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"iPublic/LoggerModular"
)

type GRpcClient struct {
	m_pClientCon *grpc.ClientConn
}

func (pThis *GRpcClient) GRpcDial(strIp string) error {
	logger := LoggerModular.GetLogger()
	pThis.Close()
	clientCon, err := grpc.Dial(strIp, grpc.WithInsecure())
	if err != nil {
		logger.Errorf("创建 GRPC 链接失败：[%v]", err)
		return err
	}
	pThis.m_pClientCon = clientCon
	return nil
}

func (pThis *GRpcClient) Close() {
	logger := LoggerModular.GetLogger()
	if nil != pThis.m_pClientCon {
		err := pThis.m_pClientCon.Close()
		if err != nil {
			logger.Errorf("GRPC 链接关闭失败：[%v]", err)
			return
		}
		pThis.m_pClientCon = nil
	}
}

func (pThis *GRpcClient) Notify(strChannelID string, strRelativePath string, strMountPoint string, strDate string, strRecordID string, nStartTime int64, nType int32) (*StorageMaintainerMessage.StreamResData, error) {
	if nil == pThis.m_pClientCon {
		return nil, errors.New("Client Has No Connected")
	}
	c := StorageMaintainerMessage.NewGreeterClient(pThis.m_pClientCon)
	req := StorageMaintainerMessage.StreamReqData{
		StrChannelID:    strChannelID,
		StrRelativePath: strRelativePath,
		StrMountPoint:   strMountPoint,
		StrDate:         strDate,
		StrRecordID:     strRecordID,
		NStartTime:      nStartTime,
		NType:           nType}
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
