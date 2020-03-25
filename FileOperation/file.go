package FileOperation

import (
	"StorageMaintainer1/DataDefine"
	"errors"
	"os"
)

func DeleteFileObject(pk string, sType int) error {
	/*
		根据不同存储类型区分删除方式
	*/
	switch sType {
	case DataDefine.StorageType_Local_NFS:
		return RemoveFileByDisk(pk)
	default:
		return errors.New("UnSupport Storage Type. ")
	}
}

func RemoveFileByDisk(path string) error {
	return os.Remove(path)
}
