package node

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	provisionerLock sync.Mutex
	provisioner     = &provision{}
)

type provision struct {
	Path string
}

func Initialize(path string) {
	provisioner.Path = path
}

func Provision(volumeID string) (string, error) {
	provisionerLock.Lock()
	defer provisionerLock.Unlock()

	if len(provisioner.Path) == 0 {
		return "", fmt.Errorf("no base path provided")
	}

	if err := os.MkdirAll(filepath.Join(provisioner.Path, volumeID), 0755); err != nil {
		return "", err
	}

	return filepath.Join(provisioner.Path, volumeID), nil
}

func Unprovision(vId string) error {
	return os.RemoveAll(filepath.Join(provisioner.Path, vId))
}