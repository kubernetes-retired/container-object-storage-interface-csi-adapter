package node

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"k8s.io/mount-utils"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client/fake"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

const (
	volumeId   = "vol-id1234567890"
	targetPath = "/data/cosi"
)

var errBoom = errors.New("boom")

func TestMountDir(t *testing.T) {
	type args struct {
		rclient    client.ProvisionerClient
		volId      string
		targetPath string
		mp         mount.Interface
	}

	type want struct {
		err error
	}

	cases := map[string]struct {
		args
		want
	}{
		"SuccessfulCreateDir": {
			args: args{
				mp: &mount.FakeMounter{
					MountPoints: []mount.MountPoint{},
				},
				volId:      volumeId,
				targetPath: targetPath,
				rclient: &fake.MockProvisionerClient{
					MockMkdirAll: func(path string, perm os.FileMode) error {
						return nil
					},
				},
			},
			want: want{
				err: nil,
			},
		},
		"SuccessfulNotMount": {
			args: args{
				mp: &mount.FakeMounter{
					MountPoints: []mount.MountPoint{},
				},
				volId:      volumeId,
				targetPath: "/var/lib",
				rclient:    &fake.MockProvisionerClient{},
			},
			want: want{
				err: nil,
			},
		},
		"FailMountPathIssue": {
			args: args{
				mp: &mount.FakeMounter{
					MountPoints: []mount.MountPoint{},
					MountCheckErrors: map[string]error{
						targetPath: errBoom,
					},
				},
				volId:      volumeId,
				targetPath: targetPath,
				rclient:    &fake.MockProvisionerClient{},
			},
			want: want{
				err: errBoom,
			},
		},
		"FailMountMkdirFailed": {
			args: args{
				mp: &mount.FakeMounter{
					MountPoints: []mount.MountPoint{},
				},
				volId:      volumeId,
				targetPath: targetPath,
				rclient: &fake.MockProvisionerClient{
					MockMkdirAll: func(path string, perm os.FileMode) error {
						return errBoom
					},
				},
			},
			want: want{
				err: errors.Wrap(errBoom, util.WrapErrorFailedToMkdirForMount),
			},
		},
		"FailIsAlreadyMountPath": {
			args: args{
				mp: &mount.FakeMounter{
					MountPoints: []mount.MountPoint{
						{
							Path: "/var/lib",
						},
					},
				},
				volId:      volumeId,
				targetPath: "/var/lib",
				rclient:    &fake.MockProvisionerClient{},
			},
			want: want{
				err: fmt.Errorf("%s is already mounted", "/var/lib"),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			p := &Provisioner{
				dataPath: "",
				mounter:  tc.mp,
				pclient:  tc.rclient,
			}

			err := p.mountDir(tc.volId, tc.targetPath)

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}
