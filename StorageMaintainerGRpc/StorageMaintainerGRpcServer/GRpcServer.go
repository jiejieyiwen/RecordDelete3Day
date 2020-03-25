package StorageMaintainerGRpcServer

//import (
//	"StorageMaintainer/TaskDispatch"
//	"StorageMaintainer1/DataManager"
//	"StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
//	"github.com/pkg/errors"
//	"github.com/sirupsen/logrus"
//	"golang.org/x/net/context"
//	"google.golang.org/grpc"
//	"google.golang.org/grpc/reflection"
//	"iPublic/LoggerModular"
//	"net"
//)
//
//type Server struct {
//	m_plooger *logrus.Entry
//	StorageMaintainerMessage.UnimplementedGreeterServer
//}

//var grpcServer Server
//
//func GetServer() *Server {
//	return &grpcServer
//}
//
//func (pThis *Server) DC_Notify(ctx context.Context, req *StorageMaintainerMessage.DC_Request) (*StorageMaintainerMessage.DC_Respon, error) {
//	if pThis.m_plooger == nil {
//		pThis.m_plooger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
//	}
//	if req == nil {
//		pThis.m_plooger.Error("request is empty~!")
//		return nil, errors.New("request is empty")
//	}
//	pThis.m_plooger.Info("收到 DC 通知~!")
//	if req.BGetNew == true {
//		DataManager.GetDataManager().SetNotifyStatus(req.BGetNew)
//		TaskDispatch.GetTaskManager().M_wg.Wait()
//		DataManager.GetDataManager().GetNewChannelStorage()
//		DataManager.GetDataManager().SetNotifyStatus(!req.BGetNew)
//		go TaskDispatch.GetTaskManager().GoStartDeleteFromQueue()
//		go TaskDispatch.GetTaskManager().GoStartQueryMongoDB()
//		return &StorageMaintainerMessage.DC_Respon{StrRespon: 1}, nil
//	}
//	return nil, nil
//}
//
//func (pThis *Server) InitServer() (err error) {
//	//创建日志
//	if pThis.m_plooger == nil {
//		pThis.m_plooger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
//	}
//
//	//建立grpc监听
//	lis, err := net.Listen("tcp", ":24516")
//	if err != nil {
//		pThis.m_plooger.Errorf("Start GRPC Listen Err: [%v]", err)
//		return err
//	}
//	pThis.m_plooger.Info("Start GRPC Listen Success~!")
//
//	//grpc服务
//	gRpcServer := grpc.NewServer()
//	StorageMaintainerMessage.RegisterDC_NotificationServer(gRpcServer, &Server{})
//	reflection.Register(gRpcServer)
//	if err := gRpcServer.Serve(lis); err != nil {
//		pThis.m_plooger.Errorf("Failed to Serve Grpc Err: [%v]", err)
//		return err
//	}
//	pThis.m_plooger.Info("Success to Serve Grpc~!")
//	return nil
//}
