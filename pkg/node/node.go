package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
)

var _ csi.NodeServer = &NodeServer{}

const (
	credsFileName    = "credentials"
	protocolFileName = "protocolConn.json"
	barNameKey       = "bar-name"
	barNamespaceKey  = "bar-namespace"
)

var getError = func(t, n string, e error) error { return fmt.Errorf("failed to get <%s>%s: %v", t, n, e) }

func NewNodeServerOrDie(driverName, nodeID, dataRoot string, volumeLimit int64) csi.NodeServer {
	client := NewClientOrDie()

	return &NodeServer{
		name:        driverName,
		nodeID:      nodeID,
		cosiClient:  client,
		provisioner: NewProvisioner(dataRoot),
		volumeLimit: volumeLimit,
	}
}

// logErr should be called at the interface method scope, prior to returning errors to the gRPC client.
func logErr(e error) error {
	klog.Error(e)
	return e
}

// NodeServer implements the NodePublishVolume and NodeUnpublishVolume methods
// of the csi.NodeServer
type NodeServer struct {
	csi.UnimplementedNodeServer
	name        string
	nodeID      string
	cosiClient  *NodeClient
	provisioner Provisioner
	volumeLimit int64
}

func (n *NodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.Infof("NodePublishVolume: volId: %v, targetPath: %v\n", request.GetVolumeId(), request.TargetPath)

	barName, barNs, err := parseVolumeContext(request.VolumeContext)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	bkt, secret, err := n.cosiClient.GetResources(ctx, barName, barNs)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	protocolConnection, err := n.cosiClient.getProtocol(bkt)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("bucket %q has protocol %q", bkt.Name, bkt.Spec.Protocol.Name)

	if err := n.provisioner.createDir(request.GetVolumeId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	creds, err := parseData(secret)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := n.provisioner.writeFileToVolume(protocolConnection, request.GetVolumeId(), protocolFileName); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := n.provisioner.writeFileToVolume(creds, request.GetVolumeId(), credsFileName); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = n.provisioner.mountDir(request.GetVolumeId(), request.GetTargetPath())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.Infof("NodeUnpublishVolume: volId: %v, targetPath: %v\n", request.GetVolumeId(), request.GetTargetPath())
	err := n.provisioner.removeDir(request.GetVolumeId())
	if err != nil {
		return nil, err
	}

	err = n.provisioner.removeMount(request.GetTargetPath())
	if err != nil {
		return nil, err
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeGetInfo(ctx context.Context, request *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	resp := &csi.NodeGetInfoResponse{
		NodeId:            n.nodeID,
		MaxVolumesPerNode: n.volumeLimit,
	}
	return resp, nil
}

func (n *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func parseData(s *v1.Secret) ([]byte, error) {
	output := make(map[string]string)
	for key, value := range s.Data {
		output[key] = string(value)
	}
	data, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return data, nil
}
