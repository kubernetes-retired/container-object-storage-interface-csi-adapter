package client

import (
	"fmt"
	"os"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

type ProvisionerClient interface {
	MkdirAll(path string, perm os.FileMode) error
	RemoveAll(path string) error
	WriteFile(data []byte, filepath string) error
}

func NewProvisionerClient() ProvisionerClient {
	return &provisionerClient{}
}

var _ ProvisionerClient = &provisionerClient{}

type provisionerClient struct{}

func (p provisionerClient) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (p provisionerClient) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (p provisionerClient) WriteFile(data []byte, filepath string) error {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, os.FileMode(0440))
	if err != nil {
		return util.LogErr(fmt.Errorf("error creating file: %s: %v", filepath, err))
	}

	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		return util.LogErr(fmt.Errorf("unable to write to file: %v", err))
	}
	return nil
}
