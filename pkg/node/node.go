package node

import (
	"context"
	"github.com/container-object-storage-interface/api/apis/objectstorage.k8s.io/v1alpha1"
	cs "github.com/container-object-storage-interface/api/clientset/typed/objectstorage.k8s.io/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/client-go/kubernetes"
)

var _ csi.NodeServer = &NodeServer{}

func NewNodeServer(driverName, nodeID string, c cs.ObjectstorageV1alpha1Client, kube kubernetes.Interface) csi.NodeServer {
	return &NodeServer{
		name:       driverName,
		nodeID:     nodeID,
		cosiClient: c,
		ctx:        context.Background(),
		kubeClient: kube,
	}
}

// NodeServer implements the NodePublishVolume and NodeUnpublishVolume methods
// of the csi.NodeServer interface and GetPluginCapabilities, GetPluginInfo, and
// Probe of the IdentityServer interface.
type NodeServer struct {
	name       string
	nodeID     string
	cosiClient cs.ObjectstorageV1alpha1Client
	kubeClient kubernetes.Interface
	ctx        context.Context
}

func (n NodeServer) getBAR(barName, barNs string) (*v1alpha1.BucketAccessRequest, error)  {
	panic("implement me")
}

func (n NodeServer) getBA(baName string) (*v1alpha1.BucketAccess, error)  {
	panic("implement me")
}

func (n NodeServer) getBR(brName, brNs string) (*v1alpha1.BucketRequest, error)  {
	panic("implement me")
}

func (n NodeServer) getB(bName string)  (*v1alpha1.Bucket, error) {
	panic("implement me")
}

func (n NodeServer) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodeGetVolumeStats(ctx context.Context, request *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodeExpandVolume(ctx context.Context, request *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodeGetCapabilities(ctx context.Context, request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodeGetInfo(ctx context.Context, request *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	panic("implement me")
}
