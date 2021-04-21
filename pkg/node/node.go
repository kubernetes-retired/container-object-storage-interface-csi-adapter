package node

import (
	"context"
	"encoding/json"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

var _ csi.NodeServer = &NodeServer{}

const (
	credsFileName    = "credentials"
	protocolFileName = "protocolConn.json"
	metadataFilename = "metadata.json"
)

func NewNodeServerOrDie(driverName, nodeID, dataRoot string, volumeLimit int64) csi.NodeServer {
	return &NodeServer{
		name:        driverName,
		nodeID:      nodeID,
		cosiClient:  client.NewClientOrDie(),
		provisioner: NewProvisioner(dataRoot, mount.New(""), client.NewProvisionerClient()),
		volumeLimit: volumeLimit,
	}
}

// NodeServer implements the NodePublishVolume and NodeUnpublishVolume methods
// of the csi.NodeServer
type NodeServer struct {
	csi.UnimplementedNodeServer
	name        string
	nodeID      string
	cosiClient  client.NodeClient
	provisioner Provisioner
	volumeLimit int64
}

func (n *NodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.Infof("NodePublishVolume: volId: %v, targetPath: %v\n", request.GetVolumeId(), request.TargetPath)

	barName, barNs, podName, podNs, err := client.ParseVolumeContext(request.VolumeContext)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	bkt, ba, secret, err := n.cosiClient.GetResources(ctx, barName, barNs)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	protocolConnection, err := n.cosiClient.GetProtocol(bkt)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	klog.Infof("bucket %q has protocol %q", bkt.Name, bkt.Spec.Protocol)

	if err := n.provisioner.createDir(request.GetVolumeId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	creds, err := util.ParseData(secret)
	if err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := n.provisioner.writeFileToVolumeMount(protocolConnection, request.GetVolumeId(), protocolFileName); err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := n.provisioner.writeFileToVolumeMount(creds, request.GetVolumeId(), credsFileName); err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = n.provisioner.mountDir(request.GetVolumeId(), request.GetTargetPath())
	if err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	meta := Metadata{
		BaName:       ba.Name,
		PodName:      podName,
		PodNamespace: podNs,
	}

	err = n.cosiClient.AddBAFinalizer(ctx, ba, meta.finalizer())
	if err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	data, err := json.Marshal(meta)
	if err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Write the BA.name to a metadata file in our volume, this is not mounted to the app pod
	if err := n.provisioner.writeFileToVolume(data, request.GetVolumeId(), metadataFilename); err != nil {
		rmErr := n.provisioner.removeDir(request.GetVolumeId())
		if rmErr != nil {
			return nil, status.Errorf(codes.Internal, "Parsing Secret Failed: %v", errors.Wrap(err, "failed to parse secret"))
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.Infof("NodeUnpublishVolume: volId: %v, targetPath: %v\n", request.GetVolumeId(), request.GetTargetPath())

	data, err := n.provisioner.readFileFromVolume(request.GetVolumeId(), metadataFilename)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	meta := Metadata{}
	err = json.Unmarshal(data, &meta)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.InfoS("read metadata file", "metadata", meta)

	ba, err := n.cosiClient.GetBA(ctx, meta.BaName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = n.cosiClient.RemoveBAFinalizer(ctx, ba, meta.finalizer())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = n.provisioner.removeMount(request.GetTargetPath())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = n.provisioner.removeDir(request.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Publish Volume Failed: %v", err)
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
