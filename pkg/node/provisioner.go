package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/utils/mount"
)

type Provisioner struct {
	dataPath string
	mounter  mount.Interface
}

func NewProvisioner(dataPath string) Provisioner {
	return Provisioner{
		dataPath: dataPath,
		mounter:  mount.New(""),
	}
}

func (p Provisioner) volPath(volID string) string {
	return filepath.Join(p.dataPath, volID)
}

func (p Provisioner) createDir(volID string) error {
	if err := os.MkdirAll(p.volPath(volID), 0750); err != nil {
		return fmt.Errorf("publish volume failed: %v", err)
	}
	return nil
}

func (p Provisioner) removeDir(volID string) error {
	if err := os.RemoveAll(p.volPath(volID)); err != nil {
		return status.Errorf(codes.Internal, "Publish Volume Failed: %v", err)
	}
	return nil
}

func (p Provisioner) mountDir(volID, targetPath string) error {
	// Check if the target path is already mounted. Prevent remounting.
	notMnt, err := mount.IsNotMountPoint(p.mounter, targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}

	if !notMnt {
		return nil
	}

	if err := p.mounter.Mount(p.volPath(volID), targetPath, "", []string{"bind"}); err != nil {
		var errList strings.Builder
		errList.WriteString(err.Error())
		if rmErr := os.RemoveAll(p.volPath(volID)); rmErr != nil && !os.IsNotExist(rmErr) {
			errList.WriteString(fmt.Sprintf(" :%s", rmErr.Error()))
		}
		return fmt.Errorf("failed to mount device: %s at %s: %s", p.volPath(volID), targetPath, errList.String())
	}

	return nil
}

func (p Provisioner) writeFileToVolume(data []byte, volID, fileName string) error {
	err := writeFile(data, filepath.Join(p.volPath(volID), fileName))
	if err != nil {
		return err
	}
	return nil
}

func (p Provisioner) removeMount(path string) error {
	err := mount.CleanupMountPoint(path, p.mounter, true)
	if err != nil && !os.IsNotExist(err) {
		klog.Error(err, "failed to clean and unmount target path", "targetPath", path)
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func writeFile(data []byte, filepath string) error {
	klog.Infof("creating conn file: %s", filepath)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, os.FileMode(0440))
	if err != nil {
		return logErr(fmt.Errorf("error creating file: %s: %v", filepath, err))
	}

	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		return logErr(fmt.Errorf("unable to write to file: %v", err))
	}

	return nil
}
