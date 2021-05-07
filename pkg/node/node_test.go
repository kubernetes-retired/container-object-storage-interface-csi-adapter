package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	testutils "sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util/test"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/go-cmp/cmp"
	"k8s.io/mount-utils"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client/fake"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

const (
	name     = "testName"
	nodeId   = "testNodeID"
	volLimit = 100

	podName = "testPodName"

	provVolumeId   = "volId-123456789"
	provTargetPath = "/var/lib/pod/secret"
)

var (
	ctx = context.Background()
)

func genRPCError(code codes.Code, err error) error {
	return status.Error(code, err.Error())
}

type ProvisionerModifier func(provisioner *Provisioner)

func getTestProvisioner(provisionerClient *fake.MockProvisionerClient, mod ...ProvisionerModifier) Provisioner {
	mounter := mount.NewFakeMounter([]mount.MountPoint{})
	prov := &Provisioner{
		dataPath: "",
		mounter:  mounter,
		pclient:  provisionerClient,
	}
	for _, v := range mod {
		v(prov)
	}
	return *prov
}

func withErrorMap(errMap map[string]error) ProvisionerModifier {
	return func(provisioner *Provisioner) {
		fm := provisioner.mounter.(*mount.FakeMounter)
		fm.MountCheckErrors = errMap
	}
}

func withMountPoints(mps []mount.MountPoint) ProvisionerModifier {
	return func(provisioner *Provisioner) {
		fm := provisioner.mounter.(*mount.FakeMounter)
		fm.MountPoints = mps
	}
}

func TestNodePublishVolume(t *testing.T) {
	type args struct {
		nclient     *fake.FakeNodeClient
		provisioner Provisioner
		request     *csi.NodePublishVolumeRequest
	}

	type want struct {
		response *csi.NodePublishVolumeResponse
		err      error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return nil
						},
						MockWriteFile: func(data []byte, filepath string) error {
							return nil
						},
						MockRemoveAll: func(path string) error {
							return nil
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						tempBar := testutils.GetBAR()
						if tempBar.Namespace == barNs && tempBar.Name == barName {
							ba = testutils.GetBA()
							bkt = testutils.GetB()
							secret = testutils.GetSecret()
						}
						return bkt, ba, secret, nil
					},
					MockAddBAFinalizer: func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
						return nil
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: &csi.NodePublishVolumeResponse{},
				err:      nil,
			},
		},
		"ErrorFailedToParseVolume": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{},
				),
				nclient: &fake.FakeNodeClient{},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.InvalidArgument, fmt.Errorf(util.ErrorTemplateVolCtxUnset, client.BarNameKey)),
			},
		},
		"ErrorInvalidBucketProtocol": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB(
							testutils.WithProtocol(v1alpha1.Protocol{}),
						)
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey: testutils.GetBAR().Name,

						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.FailedPrecondition, util.ErrorInvalidProtocol),
			},
		},
		"ErrorMkdirFailed": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return errBoom
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorMkdirFailed)),
			},
		},
		"ErrorFailedToCreateFile": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return nil
						},
						MockWriteFile: func(data []byte, filepath string) error {
							return errBoom
						},
						MockRemoveAll: func(path string) error {
							return nil
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, testutils.MultipleWrap(errBoom, util.WrapErrorFailedToCreateBucketFile, util.WrapErrorFailedToWriteProtocol)),
			},
		},
		"ErrorFailedToCreateFileRmFailed": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return nil
						},
						MockWriteFile: func(data []byte, filepath string) error {
							return errBoom
						},
						MockRemoveAll: func(path string) error {
							return errBoom
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, testutils.MultipleWrap(errBoom, util.WrapErrorFailedRemoveDirectory, util.WrapErrorFailedToWriteProtocol)),
			},
		},
		"ErrorFailedToMountDirMkdir": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							if path == provTargetPath {
								return errBoom
							}
							return nil
						},
						MockWriteFile: func(data []byte, filepath string) error {
							return nil
						},
						MockRemoveAll: func(path string) error {
							return nil
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, testutils.MultipleWrap(errBoom, util.WrapErrorFailedToMkdirForMount, util.WrapErrorFailedToMountVolume)),
			},
		},
		"ErrorFailedToMountDir": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return nil
						},
						MockWriteFile: func(data []byte, filepath string) error {
							return nil
						},
						MockRemoveAll: func(path string) error {
							return nil
						},
					}, withErrorMap(map[string]error{
						provTargetPath: errBoom,
					}),
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorFailedToMountVolume)),
			},
		},
		"ErrorFailedToAddBAFinalizer": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return nil
						},
						MockWriteFile: func(data []byte, filepath string) error {
							return nil
						},
						MockRemoveAll: func(path string) error {
							return nil
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
					MockAddBAFinalizer: func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
						return errBoom
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorFailedToAddFinalizer)),
			},
		},
		"ErrorFailedToWriteMetadata": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockMkdirAll: func(path string, perm os.FileMode) error {
							return nil
						},
						MockWriteFile: func(data []byte, fp string) error {
							if filepath.Base(fp) == metadataFilename {
								return errBoom
							}
							return nil
						},
						MockRemoveAll: func(path string) error {
							return nil
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetResources: func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
						bkt = testutils.GetB()
						ba = testutils.GetBA()
						secret = testutils.GetSecret()
						return
					},
					MockAddBAFinalizer: func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
						return nil
					},
				},
				request: &csi.NodePublishVolumeRequest{
					VolumeContext: map[string]string{
						client.BarNameKey:      testutils.GetBAR().Name,
						client.PodNameKey:      podName,
						client.PodNamespaceKey: testutils.Namespace,
					},
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, testutils.MultipleWrap(errBoom, util.WrapErrorFailedToCreateVolumeFile, util.WrapErrorFailedToWriteMetadata)),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ns := &NodeServer{
				name:        name,
				nodeID:      nodeId,
				cosiClient:  tc.nclient,
				provisioner: tc.provisioner,
				volumeLimit: volLimit,
			}

			response, err := ns.NodePublishVolume(ctx, tc.request)

			if diff := cmp.Diff(tc.want.response, response); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}

func TestNodeUnpublishVolume(t *testing.T) {
	type args struct {
		nclient     *fake.FakeNodeClient
		provisioner Provisioner
		request     *csi.NodeUnpublishVolumeRequest
	}

	type want struct {
		response *csi.NodeUnpublishVolumeResponse
		err      error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockRemoveAll: func(path string) error {
							return nil
						},
						MockReadFile: func(filename string) ([]byte, error) {
							meta := Metadata{
								BaName:       "bucketAccessName",
								PodName:      podName,
								PodNamespace: testutils.Namespace,
							}
							return json.Marshal(meta)
						},
					}, withMountPoints([]mount.MountPoint{
						{
							Path: provTargetPath,
						},
					}),
				),
				nclient: &fake.FakeNodeClient{
					MockGetBA: func(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
						tempBa := testutils.GetBA()
						if tempBa.Name == baName {
							return tempBa, nil
						}
						return nil, errBoom
					},
					MockRemoveBAFinalizer: func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
						return nil
					},
				},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: &csi.NodeUnpublishVolumeResponse{},
				err:      nil,
			},
		},
		"FailedToReadFile": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockReadFile: func(filename string) ([]byte, error) {
							return nil, errBoom
						},
					},
				),
				nclient: &fake.FakeNodeClient{},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorFailedToReadMetadataFile)),
			},
		},
		"FailedToUnmarshalMetadataFile": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockReadFile: func(filename string) ([]byte, error) {
							s := "{"
							return []byte(s), nil
						},
					},
				),
				nclient: &fake.FakeNodeClient{},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errors.New("unexpected end of JSON input"), util.WrapErrorFailedToUnmarshalMetadata)),
			},
		},
		"FailedToGetBA": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockReadFile: func(filename string) ([]byte, error) {
							meta := Metadata{
								BaName:       "wrongName",
								PodName:      podName,
								PodNamespace: testutils.Namespace,
							}
							return json.Marshal(meta)
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetBA: func(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
						tempBa := testutils.GetBA()
						if tempBa.Name == baName {
							return tempBa, nil
						}
						return nil, errBoom
					},
				},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errBoom),
			},
		},
		"FailedToRemoveMount": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockRemoveAll: func(path string) error {
							return nil
						},
						MockReadFile: func(filename string) ([]byte, error) {
							meta := Metadata{
								BaName:       "bucketAccessName",
								PodName:      podName,
								PodNamespace: testutils.Namespace,
							}
							return json.Marshal(meta)
						},
					}, withErrorMap(map[string]error{
						"/var/lib": errBoom,
					}),
				),
				nclient: &fake.FakeNodeClient{
					MockGetBA: func(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
						return testutils.GetBA(), nil
					},
				},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: "/var/lib",
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorFailedToUnmountVolume)),
			},
		},
		"FailedToRemoveDir": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockRemoveAll: func(path string) error {
							return errBoom
						},
						MockReadFile: func(filename string) ([]byte, error) {
							meta := Metadata{
								BaName:       "bucketAccessName",
								PodName:      podName,
								PodNamespace: testutils.Namespace,
							}
							return json.Marshal(meta)
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetBA: func(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
						return testutils.GetBA(), nil
					},
				},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorFailedToRemoveDir)),
			},
		},
		"FailedToRemoveFinalizer": {
			args: args{
				provisioner: getTestProvisioner(
					&fake.MockProvisionerClient{
						MockRemoveAll: func(path string) error {
							return nil
						},
						MockReadFile: func(filename string) ([]byte, error) {
							meta := Metadata{
								BaName:       "bucketAccessName",
								PodName:      podName,
								PodNamespace: testutils.Namespace,
							}
							return json.Marshal(meta)
						},
					},
				),
				nclient: &fake.FakeNodeClient{
					MockGetBA: func(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
						return testutils.GetBA(), nil
					},
					MockRemoveBAFinalizer: func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
						return errBoom
					},
				},
				request: &csi.NodeUnpublishVolumeRequest{
					VolumeId:   provVolumeId,
					TargetPath: provTargetPath,
				},
			},
			want: want{
				response: nil,
				err:      genRPCError(codes.Internal, errors.Wrap(errBoom, util.WrapErrorFailedToRemoveFinalizer)),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ns := &NodeServer{
				name:        name,
				nodeID:      nodeId,
				cosiClient:  tc.nclient,
				provisioner: tc.provisioner,
				volumeLimit: volLimit,
			}

			response, err := ns.NodeUnpublishVolume(ctx, tc.request)

			if diff := cmp.Diff(tc.want.response, response); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}
