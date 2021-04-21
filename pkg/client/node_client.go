package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	cs "sigs.k8s.io/container-object-storage-interface-api/clientset/typed/objectstorage.k8s.io/v1alpha1"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

const (
	podNameKey      = "csi.storage.k8s.io/pod.name"
	podNamespaceKey = "csi.storage.k8s.io/pod.namespace"

	barNameKey      = "bar-name"
	barNamespaceKey = "bar-namespace"
)

var _ NodeClient = &nodeClient{}

type nodeClient struct {
	cosiClient *cs.ObjectstorageV1alpha1Client
	kubeClient kubernetes.Interface
}

type NodeClient interface {
	GetBAR(ctx context.Context, barName, barNs string) (*v1alpha1.BucketAccessRequest, error)
	GetBA(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error)
	GetBR(ctx context.Context, brName, brNs string) (*v1alpha1.BucketRequest, error)
	GetB(ctx context.Context, bName string) (*v1alpha1.Bucket, error)

	GetResources(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error)
	GetProtocol(bkt *v1alpha1.Bucket) (data []byte, err error)

	AddBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
	RemoveBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
}

func NewClientOrDie() NodeClient {
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
	}
}

func ParseVolumeContext(volCtx map[string]string) (barname, barns, podname, podns string, err error) {
	klog.Info("parsing bucketAccessRequest namespace/name from volume context")
	if barname, err = util.ParseValue(barNameKey, volCtx); err != nil {
		return "", "", "", "", err
	}
	if barns, err = util.ParseValue(barNamespaceKey, volCtx); err != nil {
		return "", "", "", "", err
	}
	if podname, err = util.ParseValue(podNameKey, volCtx); err != nil {
		return "", "", "", "", err
	}
	if podns, err = util.ParseValue(podNamespaceKey, volCtx); err != nil {
		return "", "", "", "", err
	}
	return barname, barns, podname, podns, nil
}

func (n *nodeClient) GetBAR(ctx context.Context, barName, barNs string) (*v1alpha1.BucketAccessRequest, error) {
	klog.Infof("getting bucketAccessRequest %q", fmt.Sprintf("%s/%s", barNs, barName))
	bar, err := n.cosiClient.BucketAccessRequests(barNs).Get(ctx, barName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "get bucketAccessRequest failed")
	}
	if bar == nil {
		return nil, fmt.Errorf("bucketAccessRequest is nil %q", fmt.Sprintf("%s/%s", barNs, barName))
	}
	if !bar.Status.AccessGranted {
		return nil, fmt.Errorf("bucketAccessRequest does not grant access %q", fmt.Sprintf("%s/%s", barNs, barName))
	}
	if len(bar.Spec.BucketRequestName) == 0 {
		return nil, fmt.Errorf("bucketAccessRequest.Spec.BucketRequestName unset")
	}
	if len(bar.Status.BucketAccessName) == 0 {
		return nil, fmt.Errorf("bucketAccessRequest.Spec.BucketAccessName unset")
	}
	return bar, nil
}

func (n *nodeClient) GetBA(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
	klog.Infof("getting bucketAccess %q", fmt.Sprintf("%s", baName))
	ba, err := n.cosiClient.BucketAccesses().Get(ctx, baName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(util.GetError("bucketAccess", baName, err))
	}
	if ba == nil {
		return nil, util.LogErr(fmt.Errorf("bucketAccess is nil %q", fmt.Sprintf("%s", baName)))
	}
	if !ba.Status.AccessGranted {
		return nil, util.LogErr(fmt.Errorf("bucketAccess does not grant access %q", fmt.Sprintf("%s", baName)))
	}
	if ba.Status.MintedSecret == nil {
		return nil, util.LogErr(fmt.Errorf("bucketAccess.Spec.MintedSecretName unset"))
	}
	return ba, nil
}

func (n *nodeClient) GetBR(ctx context.Context, brName, brNs string) (*v1alpha1.BucketRequest, error) {
	klog.Infof("getting bucketRequest %q", brName)
	br, err := n.cosiClient.BucketRequests(brNs).Get(ctx, brName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(util.GetError("bucketRequest", fmt.Sprintf("%s/%s", brNs, brName), err))
	}
	if br == nil {
		return nil, util.LogErr(fmt.Errorf("bucketRequest is nil %q", fmt.Sprintf("%s/%s", brNs, brName)))
	}
	if !br.Status.BucketAvailable {
		return nil, util.LogErr(fmt.Errorf("bucketRequest is not available yet %q", fmt.Sprintf("%s/%s", brNs, brName)))
	}
	if len(br.Status.BucketName) == 0 {
		return nil, util.LogErr(fmt.Errorf("bucketRequest.Spec.BucketInstanceName unset"))
	}
	return br, nil
}

func (n *nodeClient) GetB(ctx context.Context, bName string) (*v1alpha1.Bucket, error) {
	klog.Infof("getting bucket %q", bName)
	// is BucketInstanceName the correct field, or should it be BucketClass
	bkt, err := n.cosiClient.Buckets().Get(ctx, bName, metav1.GetOptions{})
	if err != nil {
		return nil, util.LogErr(util.GetError("bucket", bName, err))
	}
	if bkt == nil {
		return nil, util.LogErr(fmt.Errorf("bucket is nil %q", fmt.Sprintf("%s", bName)))
	}
	if !bkt.Status.BucketAvailable {
		return nil, util.LogErr(fmt.Errorf("bucket is not available yet %q", fmt.Sprintf("%s", bName)))
	}
	return bkt, nil
}

func (n *nodeClient) GetResources(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
	var bar *v1alpha1.BucketAccessRequest

	if bar, err = n.GetBAR(ctx, barName, barNs); err != nil {
		return
	}

	if ba, err = n.GetBA(ctx, bar.Status.BucketAccessName); err != nil {
		return
	}

	if bkt, err = n.GetB(ctx, ba.Spec.BucketName); err != nil {
		return
	}

	if secret, err = n.kubeClient.CoreV1().Secrets(ba.Status.MintedSecret.Namespace).Get(ctx, ba.Status.MintedSecret.Name, metav1.GetOptions{}); err != nil {
		_ = util.LogErr(util.GetError("secret", fmt.Sprintf("%s/%s", ba.Status.MintedSecret.Namespace, ba.Status.MintedSecret.Name), err))
		return
	}
	return
}

func (n *nodeClient) GetProtocol(bkt *v1alpha1.Bucket) (data []byte, err error) {
	klog.Infof("bucket protocol %+v", bkt.Spec.Protocol)
	var protocolConnection interface{}
	switch {
	case bkt.Spec.Protocol.S3 != nil:
		protocolConnection = bkt.Spec.Protocol.S3
	case bkt.Spec.Protocol.AzureBlob != nil:
		protocolConnection = bkt.Spec.Protocol.AzureBlob
	case bkt.Spec.Protocol.GCS != nil:
		protocolConnection = bkt.Spec.Protocol.GCS
	default:
		err = fmt.Errorf("unrecognized protocol %+v, unable to extract connection data", bkt.Spec.Protocol)
	}

	if err != nil {
		return nil, util.LogErr(err)
	}

	if data, err = json.Marshal(protocolConnection); err != nil {
		return nil, util.LogErr(err)
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
