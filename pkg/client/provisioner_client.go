package client

import "os"

type ProvisionerClient interface {
	MkdirAll(path string, perm os.FileMode) error
	RemoveAll(path string) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
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

func (p provisionerClient) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}
