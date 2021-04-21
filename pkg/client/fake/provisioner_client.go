package fake

import (
	"os"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
)

var _ client.ProvisionerClient = &MockProvisionerClient{}

type MockProvisionerClient struct {
	MockMkdirAll  func(path string, perm os.FileMode) error
	MockRemoveAll func(path string) error
	MockOpenFile  func(name string, flag int, perm os.FileMode) (*os.File, error)
}

func (p MockProvisionerClient) MkdirAll(path string, perm os.FileMode) error {
	return p.MockMkdirAll(path, perm)
}

func (p MockProvisionerClient) RemoveAll(path string) error {
	return p.MockRemoveAll(path)
}

func (p MockProvisionerClient) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return p.MockOpenFile(name, flag, perm)
}
