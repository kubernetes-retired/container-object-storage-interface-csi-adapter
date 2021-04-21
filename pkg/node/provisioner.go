package node

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

const (
	finalizer = "cosi.objectstorage.k8s.io/bucketaccess-protection"
)

type Provisioner struct {
	dataPath string
	mounter  mount.Interface
	pclient  client.ProvisionerClient
}

func NewProvisioner(dataPath string, p mount.Interface, pc client.ProvisionerClient) Provisioner {
	return Provisioner{
		dataPath: dataPath,
		mounter:  p,
		pclient:  pc,
	}
}

func (p Provisioner) volPath(volID string) string {
	return filepath.Join(p.dataPath, volID)
}

func (p Provisioner) bucketPath(volID string) string {
	return filepath.Join(p.dataPath, volID, "bucket")
}

func (p Provisioner) createDir(volID string) error {
	if err := p.pclient.MkdirAll(p.bucketPath(volID), 0750); err != nil {
		return fmt.Errorf("publish volume failed: %v", err)
	}
	return nil
}

func (p Provisioner) removeDir(volID string) error {
	if err := p.pclient.RemoveAll(p.volPath(volID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (p Provisioner) mountDir(volID, targetPath string) error {
	// Check if the target path is already mounted. Prevent remounting.
	notMnt, err := mount.IsNotMountPoint(p.mounter, targetPath)
	if err != nil {
		klog.Error(err)
		if os.IsNotExist(err) {
			if err = p.pclient.MkdirAll(targetPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}

	if !notMnt {
		return fmt.Errorf("%s is already mounted", targetPath)
	}

	if err := p.mounter.Mount(p.bucketPath(volID), targetPath, "", []string{"bind"}); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to mount device: %s at %s", p.bucketPath(volID), targetPath))
	}
	return nil
}

func (p Provisioner) writeFileToVolumeMount(data []byte, volID, fileName string) error {
	err := p.writeFile(data, filepath.Join(p.bucketPath(volID), fileName))
	if err != nil {
		return err
	}
	return nil
}

func (p Provisioner) writeFileToVolume(data []byte, volID, fileName string) error {
	err := p.writeFile(data, filepath.Join(p.volPath(volID), fileName))
	if err != nil {
		return err
	}
	return nil
}

func (p Provisioner) readFileFromVolume(volID, fileName string) ([]byte, error) {
	return ioutil.ReadFile(filepath.Join(p.volPath(volID), fileName))
}

func (p Provisioner) removeMount(path string) error {
	err := mount.CleanupMountPoint(path, p.mounter, true)
	if err != nil && !os.IsNotExist(err) {
		klog.Error(err, "failed to clean and unmount target path", "targetPath", path)
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func (p Provisioner) writeFile(data []byte, filepath string) error {
	klog.Infof("creating conn file: %s", filepath)

	file, err := p.pclient.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, os.FileMode(0440))
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

type Metadata struct {
	BaName       string `json:"baName"`
	PodName      string `json:"podName"`
	PodNamespace string `json:"podNamespace"`
}

func (m Metadata) finalizer() string {
	return fmt.Sprintf("%s-%s-%s", finalizer, m.PodNamespace, m.PodName)
}
