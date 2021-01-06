package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/container-object-storage-interface/api/apis/objectstorage.k8s.io/v1alpha1"
	cs "github.com/container-object-storage-interface/api/clientset/typed/objectstorage.k8s.io/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"k8s.io/utils/mount"
)

var _ csi.NodeServer = &NodeServer{}

const protocolFileName string = `protocolConn.json`

var getError = func(t, n string, e error) error { return fmt.Errorf("failed to get <%s>%s: %v", t, n, e) }

func NewNodeServer(driverName, nodeID string, c cs.ObjectstorageV1alpha1Client, kube kubernetes.Interface) csi.NodeServer {
	return &NodeServer{
		name:       driverName,
		nodeID:     nodeID,
		cosiClient: c,
		ctx:        context.Background(),
		kubeClient: kube,
	}
}

// logErr should be called at the interface method scope, prior to returning errors to the gRPC client.
func logErr(e error) error {
	klog.Error(e)
	return e
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

func (n NodeServer) getBAR(barName, barNs string) (*v1alpha1.BucketAccessRequest, error) {
	klog.Infof("getting bucketAccessRequest %s", fmt.Sprintf("%s/%s", barNs, barName))
	bar, err := n.cosiClient.BucketAccessRequests(barNs).Get(n.ctx, barName, metav1.GetOptions{})
	if err != nil || bar == nil || !bar.Status.AccessGranted {
		return nil, logErr(getError("bucketAccessRequest", fmt.Sprintf("%s/%s", barNs, barName), err))
	}
	if len(bar.Spec.BucketRequestName) == 0 {
		return nil, logErr(fmt.Errorf("bucketAccessRequest.Spec.BucketRequestName unset"))
	}
	return bar, nil
}

func (n NodeServer) getBA(baName string) (*v1alpha1.BucketAccess, error) {
	klog.Infof("getting bucketAccess %s", fmt.Sprintf("%s", baName))
	ba, err := n.cosiClient.BucketAccesses().Get(n.ctx, baName, metav1.GetOptions{})
	if err != nil || ba == nil || !ba.Status.AccessGranted {
		return nil, logErr(getError("bucketAccess", fmt.Sprintf("%s", baName), err))
	}
	return ba, nil
}

func (n NodeServer) getBR(brName, brNs string) (*v1alpha1.BucketRequest, error) {
	klog.Infof("getting bucketRequest %s", brName)
	br, err := n.cosiClient.BucketRequests(brNs).Get(n.ctx, brName, metav1.GetOptions{})
	if err != nil || br == nil || !br.Status.BucketAvailable {
		return nil, logErr(getError("bucketRequest", fmt.Sprintf("%s/%s", brNs, brName), err))
	}
	return br, nil
}

func (n NodeServer) getB(bName string) (*v1alpha1.Bucket, error) {
	klog.Infof("getting bucket %s", bName)
	// is BucketInstanceName the correct field, or should it be BucketClass
	bkt, err := n.cosiClient.Buckets().Get(n.ctx, bName, metav1.GetOptions{})
	if err != nil || bkt == nil || !bkt.Status.BucketAvailable {
		return nil, logErr(getError("bucket", bName, err))
	}
	return bkt, nil
}

func (n NodeServer) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.Infof("NodeStageVolume: volId: %v, targetPath: %v\n", request.GetVolumeId(), request.StagingTargetPath)

	name, ns, err := parseVolumeContext(request.VolumeContext)
	if err != nil {
		return nil, err
	}

	pod, err := n.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, logErr(getError("pod", fmt.Sprintf("%s/%s", ns, name), err))
	}

	barName, barNs, err := parsePod(pod, n.name)

	if err != nil {
		return nil, err
	}
	bar, err := n.getBAR(barName, barNs)
	if err != nil {
		return nil, err
	}
	ba, err := n.getBA(bar.Spec.BucketAccessName)
	if err != nil {
		return nil, err
	}
	br, err := n.getBR(bar.Spec.BucketRequestName, barNs)
	if err != nil {
		return nil, err
	}

	bkt, err := n.getB(br.Spec.BucketInstanceName)
	if err != nil {
		return nil, err
	}
	secret, err := n.kubeClient.CoreV1().Secrets(barNs).Get(ctx, ba.Spec.MintedSecretName, metav1.GetOptions{})
	if err != nil {
		return nil, logErr(getError("pod", fmt.Sprintf("%s/%s", barNs, ba.Spec.MintedSecretName), err))
	}
	var protocolConnection interface{}
	switch bkt.Spec.Protocol.ProtocolName {
	case v1alpha1.ProtocolNameS3:
		protocolConnection = bkt.Spec.Protocol.S3
	case v1alpha1.ProtocolNameAzure:
		protocolConnection = bkt.Spec.Protocol.AzureBlob
	case v1alpha1.ProtocolNameGCS:
		protocolConnection = bkt.Spec.Protocol.GCS
	case "":
		err = fmt.Errorf("bucket protocol not signature")
	default:
		err = fmt.Errorf("unrecognized protocol %s, unable to extract connection data", bkt.Spec.Protocol.ProtocolName)
	}

	if err != nil {
		return nil, logErr(err)
	}
	klog.Infof("bucket %s has protocol %s", bkt.Name, bkt.Spec.Protocol.ProtocolName)

	data := make(map[string]interface{})
	data["protocol"] = protocolConnection
	data["connection"] = secret.Data

	protoData, err := json.Marshal(data)
	if err != nil {
		return nil, logErr(fmt.Errorf("error marshalling protocol: %v", err))
	}

	target := filepath.Join(request.StagingTargetPath, protocolFileName)
	klog.Infof("creating conn file: %s", target)
	f, err := os.Open(target)
	if err != nil {
		return nil, logErr(fmt.Errorf("error creating file: %s: %v", target, err))
	}
	defer f.Close()
	_, err = f.Write(protoData)
	if err != nil {
		return nil, logErr(fmt.Errorf("unable to write to file: %v", err))
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

const (
	podNameKey      = "csi.storage.k8s.io/pod.name"
	podNamespaceKey = "csi.storage.k8s.io/pod.namespace"
	barNameKey      = "bar-name"
	barNamespaceKey = "bar-namespace"
)

func parseValue(key string, ctx map[string]string) (string, error) {
	value, ok := ctx[key]
	if !ok {
		return "", fmt.Errorf("required volume context key unset: %v", key)
	}
	klog.Infof("got value: %v", value)
	return value, nil
}

func parseVolumeContext(volCtx map[string]string) (name, ns string, err error) {
	klog.Info("parsing bucketAccessRequest namespace/name from volume context")

	name, err = parseValue(podNameKey, volCtx)
	if err != nil {
		return "", "", err
	}

	ns, err = parseValue(podNamespaceKey, volCtx)
	if err != nil {
		return "", "", err
	}

	return
}

func parsePod(pod *v1.Pod, driverName string) (name, ns string, err error) {
	klog.Info("parsing bucketAccessRequest namespace/name from pod")

	for _, v := range pod.Spec.Volumes {
		if v.CSI != nil && v.CSI.Driver == driverName {
			name, ok := v.CSI.VolumeAttributes[barNameKey]
			if !ok {
				return "", "", errors.New("invalid BAR Name")
			}
			namespace, ok := v.CSI.VolumeAttributes[barNamespaceKey]
			if !ok {
				return "", "", errors.New("invalid BAR Namespace")
			}
			return name, namespace, nil
		}
	}

	return "", "", nil
}

func (n NodeServer) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	panic("implement me")
}

func (n NodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	vID := request.GetVolumeId()
	stagingTargetPath := request.GetStagingTargetPath()
	targetPath := request.GetTargetPath()

	if vID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID missing in request")
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "Stage Volume Failed: %v", err)
	}

	if err := mount.New("").Mount(stagingTargetPath, targetPath, "", []string{"bind"}); err != nil {
		return nil, status.Errorf(codes.Internal, "Publish Volume Mount Failed: %v", err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n NodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.Infof("NodeUnpublishVolume: volId: %v, targetPath: %v\n", request.GetVolumeId(), request.GetTargetPath())
	target := filepath.Join(request.TargetPath, protocolFileName)
	err := os.RemoveAll(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, logErr(fmt.Errorf("unable to remove file %s: %v", target, err))
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
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
	klog.Infof("NodeGetInfo()")
	resp := &csi.NodeGetInfoResponse{
		NodeId: n.nodeID,
	}
	return resp, nil
}
