package fake

import (
	"os"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
)

var _ client.ProvisionerClient = &MockProvisionerClient{}

type MockProvisionerClient struct {
	MockMkdirAll  func(path string, perm os.FileMode) error
	MockRemoveAll func(path string) error
	MockWriteFile func(data []byte, filepath string) error
	MockReadFile  func(filename string) ([]byte, error)
}

func (p MockProvisionerClient) ReadFile(filename string) ([]byte, error) {
	return p.MockReadFile(filename)
}

func (p MockProvisionerClient) MkdirAll(path string, perm os.FileMode) error {
	return p.MockMkdirAll(path, perm)
}

func (p MockProvisionerClient) RemoveAll(path string) error {
	return p.MockRemoveAll(path)
}

func (p MockProvisionerClient) WriteFile(data []byte, filepath string) error {
	return p.MockWriteFile(data, filepath)
}
