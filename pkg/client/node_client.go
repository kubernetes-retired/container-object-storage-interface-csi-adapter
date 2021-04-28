package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	cs "sigs.k8s.io/container-object-storage-interface-api/clientset/typed/objectstorage.k8s.io/v1alpha1"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

const (
	PodNameKey      = "csi.storage.k8s.io/pod.name"
	PodNamespaceKey = "csi.storage.k8s.io/pod.namespace"

	BarNameKey = "bar-name"
)

var _ NodeClient = &nodeClient{}

func newRecorder(kubeClient *kubernetes.Clientset, driverName, nodeID string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: driverName, Host: nodeID})
}

type nodeClient struct {
	cosiClient cs.ObjectstorageV1alpha1Interface
	kubeClient kubernetes.Interface
	recorder   record.EventRecorder
}

type NodeClient interface {
	GetBAR(ctx context.Context, pod *v1.Pod, barName, barNs string) (*v1alpha1.BucketAccessRequest, error)
	GetBA(ctx context.Context, pod *v1.Pod, baName string) (*v1alpha1.BucketAccess, error)
	GetBR(ctx context.Context, pod *v1.Pod, brName, brNs string) (*v1alpha1.BucketRequest, error)
	GetB(ctx context.Context, pod *v1.Pod, bName string) (*v1alpha1.Bucket, error)
	GetPod(ctx context.Context, podName, podNs string) (*v1.Pod, error)

	GetResources(ctx context.Context, barName, podName, podNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, pod *v1.Pod, err error)

	AddBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
	RemoveBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error

	Recorder() record.EventRecorder
}

func NewClientOrDie(driverName, nodeId string) NodeClient {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// The following function calls may panic based on the config
	client := cs.NewForConfigOrDie(config)
	kube := kubernetes.NewForConfigOrDie(config)
	return &nodeClient{
		cosiClient: client,
		kubeClient: kube,
		recorder:   newRecorder(kube, driverName, nodeId),
	}
}

func ParseVolumeContext(volCtx map[string]string) (barname, podname, podns string, err error) {
	klog.Info("parsing bucketAccessRequest namespace/name from volume context")

	if barname, err = util.ParseValue(BarNameKey, volCtx); err != nil {
		return
	}
	if podname, err = util.ParseValue(PodNameKey, volCtx); err != nil {
		return
	}
	if podns, err = util.ParseValue(PodNamespaceKey, volCtx); err != nil {
		return
	}
	return barname, podname, podns, nil
}

func (n *nodeClient) GetBAR(ctx context.Context, pod *v1.Pod, barName, barNs string) (*v1alpha1.BucketAccessRequest, error) {
	klog.Infof("getting bucketAccessRequest %q", fmt.Sprintf("%s/%s", barNs, barName))
	bar, err := n.cosiClient.BucketAccessRequests(barNs).Get(ctx, barName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(errors.Wrap(err, util.WrapErrorGetBARFailed))
	}
	// TODO: BAR.Spec.BucketRequestName can be unset if the BucketName is set
	if len(bar.Spec.BucketRequestName) == 0 {
		util.EmitWarningEvent(n.recorder, pod, util.BARBucketRequestNotSet)
		return nil, util.LogErr(util.ErrorBARUnsetBR)
	}
	if !bar.Status.AccessGranted {
		util.EmitWarningEvent(n.recorder, pod, util.BARAccessNotGranted)
		return nil, util.LogErr(util.ErrorBARNoAccess)
	}
	if len(bar.Status.BucketAccessName) == 0 {
		util.EmitWarningEvent(n.recorder, pod, util.BARBucketAccessNotSet)
		return nil, util.LogErr(util.ErrorBARUnsetBA)
	}
	return bar, nil
}

func (n *nodeClient) GetBA(ctx context.Context, pod *v1.Pod, baName string) (*v1alpha1.BucketAccess, error) {
	klog.Infof("getting bucketAccess %q", fmt.Sprintf("%s", baName))
	ba, err := n.cosiClient.BucketAccesses().Get(ctx, baName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(errors.Wrap(err, util.WrapErrorGetBAFailed))
	}
	if !ba.Status.AccessGranted {
		util.EmitWarningEvent(n.recorder, pod, util.BAAccessNotGranted)
		return nil, util.LogErr(util.ErrorBANoAccess)
	}
	if ba.Status.MintedSecret == nil {
		util.EmitWarningEvent(n.recorder, pod, util.BAMintedSecretNotSet)
		return nil, util.LogErr(util.ErrorBANoMintedSecret)
	}
	return ba, nil
}

func (n *nodeClient) GetBR(ctx context.Context, pod *v1.Pod, brName, brNs string) (*v1alpha1.BucketRequest, error) {
	klog.Infof("getting bucketRequest %q", brName)
	br, err := n.cosiClient.BucketRequests(brNs).Get(ctx, brName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(errors.Wrap(err, util.WrapErrorGetBRFailed))
	}
	if !br.Status.BucketAvailable {
		util.EmitWarningEvent(n.recorder, pod, util.BRNotAvailable)
		return nil, util.LogErr(util.ErrorBRNotAvailable)
	}
	if len(br.Status.BucketName) == 0 {
		util.EmitWarningEvent(n.recorder, pod, util.BRBucketNameNotSet)
		return nil, util.LogErr(util.ErrorBRUnsetBucketName)
	}
	return br, nil
}

func (n *nodeClient) GetB(ctx context.Context, pod *v1.Pod, bName string) (*v1alpha1.Bucket, error) {
	klog.Infof("getting bucket %q", bName)
	// is BucketInstanceName the correct field, or should it be BucketClass
	bkt, err := n.cosiClient.Buckets().Get(ctx, bName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(errors.Wrap(err, util.WrapErrorGetBFailed))
	}
	if !bkt.Status.BucketAvailable {
		util.EmitWarningEvent(n.recorder, pod, util.BNotAvailable)
		return nil, util.LogErr(util.ErrorBNotAvailable)
	}
	return bkt, nil
}

func (n *nodeClient) GetPod(ctx context.Context, podName, podNs string) (*v1.Pod, error) {
	return n.kubeClient.CoreV1().Pods(podNs).Get(ctx, podName, metav1.GetOptions{})
}

func (n *nodeClient) GetResources(ctx context.Context, barName, podName, podNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, pod *v1.Pod, err error) {
	var bar *v1alpha1.BucketAccessRequest

	if pod, err = n.GetPod(ctx, podName, podNs); err != nil {
		return
	}

	if bar, err = n.GetBAR(ctx, pod, barName, podNs); err != nil {
		return
	}

	if ba, err = n.GetBA(ctx, pod, bar.Status.BucketAccessName); err != nil {
		return
	}

	if bkt, err = n.GetB(ctx, pod, ba.Spec.BucketName); err != nil {
		return
	}

	if secret, err = n.kubeClient.CoreV1().Secrets(ba.Status.MintedSecret.Namespace).Get(ctx, ba.Status.MintedSecret.Name, metav1.GetOptions{}); err != nil {
		util.EmitWarningEvent(n.recorder, pod, util.MintedSecretNotFound)
		err = errors.Wrap(err, util.WrapErrorGetSecretFailed)
		return
	}
	util.EmitNormalEvent(n.recorder, pod, util.AllResourcesReady)
	return
}

func GetProtocol(bkt *v1alpha1.Bucket) ([]byte, error) {
	klog.Infof("bucket protocol %+v", bkt.Spec.Protocol)
	var (
		data               []byte
		err                error
		protocolConnection interface{}
	)

	switch {
	case bkt.Spec.Protocol.S3 != nil:
		protocolConnection = bkt.Spec.Protocol.S3
	case bkt.Spec.Protocol.AzureBlob != nil:
		protocolConnection = bkt.Spec.Protocol.AzureBlob
	case bkt.Spec.Protocol.GCS != nil:
		protocolConnection = bkt.Spec.Protocol.GCS
	default:
		err = util.ErrorInvalidProtocol
	}

	if err != nil {
		return nil, util.LogErr(err)
	}

	if data, err = json.Marshal(protocolConnection); err != nil {
		return nil, util.LogErr(errors.Wrap(err, util.WrapErrorMarshalProtocolFailed))
	}
	return data, nil
}

func (n *nodeClient) AddBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
	controllerutil.AddFinalizer(ba, BAFinalizer)
	if _, err := n.cosiClient.BucketAccesses().Update(ctx, ba, metav1.UpdateOptions{}); err != nil {
		return err
	}
	return nil
}

func (n *nodeClient) RemoveBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
	controllerutil.RemoveFinalizer(ba, BAFinalizer)
	if _, err := n.cosiClient.BucketAccesses().Update(ctx, ba, metav1.UpdateOptions{}); err != nil {
		return err
	}
	return nil
}

func (n *nodeClient) Recorder() record.EventRecorder {
	return n.recorder
}
