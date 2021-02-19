package node

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"

	"github.com/kubernetes-sigs/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	cs "github.com/kubernetes-sigs/container-object-storage-interface-api/clientset/typed/objectstorage.k8s.io/v1alpha1"
)

type NodeClient struct {
	cosiClient *cs.ObjectstorageV1alpha1Client
	kubeClient kubernetes.Interface
}

func NewClientOrDie() *NodeClient {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// The following function calls may panic based on the config
	client := cs.NewForConfigOrDie(config)
	kube := kubernetes.NewForConfigOrDie(config)
	return &NodeClient{
		cosiClient: client,
		kubeClient: kube,
	}
}

func parseValue(key string, volCtx map[string]string) (string, error) {
	value, ok := volCtx[key]
	if !ok {
		return "", fmt.Errorf("required volume context key unset: %v", key)
	}
	return value, nil
}

func parseVolumeContext(volCtx map[string]string) (name, ns string, err error) {
	klog.Info("parsing bucketAccessRequest namespace/name from volume context")
	if name, err = parseValue(barNameKey, volCtx); err != nil {
		return "", "", err
	}
	if ns, err = parseValue(barNamespaceKey, volCtx); err != nil {
		return "", "", err
	}
	return name, ns, nil
}

func (n *NodeClient) getBAR(ctx context.Context, barName, barNs string) (*v1alpha1.BucketAccessRequest, error) {
	klog.Infof("getting bucketAccessRequest %q", fmt.Sprintf("%s/%s", barNs, barName))
	bar, err := n.cosiClient.BucketAccessRequests(barNs).Get(ctx, barName, metav1.GetOptions{})
	if err != nil {
		return nil, logErr(getError("bucketAccessRequest", fmt.Sprintf("%s/%s", barNs, barName), err))
	}
	// TODO: need to enable validation after resolving status issue - Krish
	if bar == nil {
		return nil, logErr(fmt.Errorf("bucketAccessRequest is nil %q", fmt.Sprintf("%s/%s", barNs, barName)))
	}
	if !bar.Status.AccessGranted {
		return nil, logErr(fmt.Errorf("bucketAccessRequest does not grant access %q", fmt.Sprintf("%s/%s", barNs, barName)))
	}
	if len(bar.Spec.BucketRequestName) == 0 {
		return nil, logErr(fmt.Errorf("bucketAccessRequest.Spec.BucketRequestName unset"))
	}
	if len(bar.Spec.BucketAccessName) == 0 {
		return nil, logErr(fmt.Errorf("bucketAccessRequest.Spec.BucketAccessName unset"))
	}
	return bar, nil
}

func (n *NodeClient) getBA(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
	klog.Infof("getting bucketAccess %q", fmt.Sprintf("%s", baName))
	ba, err := n.cosiClient.BucketAccesses().Get(ctx, baName, metav1.GetOptions{})
	if err != nil {
		return nil, logErr(getError("bucketAccess", baName, err))
	}
	// TODO: need to enable validation after resolving status issue - Krish
	if ba == nil {
		return nil, logErr(fmt.Errorf("bucketAccess is nil %q", fmt.Sprintf("%s", baName)))
	}
	if !ba.Status.AccessGranted {
		return nil, logErr(fmt.Errorf("bucketAccess does not grant access %q", fmt.Sprintf("%s", baName)))
	}
	if len(ba.Spec.MintedSecretName) == 0 {
		return nil, logErr(fmt.Errorf("bucketAccess.Spec.MintedSecretName unset"))
	}
	return ba, nil
}

func (n *NodeClient) getBR(ctx context.Context, brName, brNs string) (*v1alpha1.BucketRequest, error) {
	klog.Infof("getting bucketRequest %q", brName)
	br, err := n.cosiClient.BucketRequests(brNs).Get(ctx, brName, metav1.GetOptions{})
	if err != nil {
		return nil, logErr(getError("bucketRequest", fmt.Sprintf("%s/%s", brNs, brName), err))
	}
	// TODO: need to enable validation after resolving status issue - Krish
	if br == nil {
		return nil, logErr(fmt.Errorf("bucketRequest is nil %q", fmt.Sprintf("%s/%s", brNs, brName)))
	}
	if !br.Status.BucketAvailable {
		return nil, logErr(fmt.Errorf("bucketRequest is not available yet %q", fmt.Sprintf("%s/%s", brNs, brName)))
	}
	if len(br.Spec.BucketInstanceName) == 0 {
		return nil, logErr(fmt.Errorf("bucketRequest.Spec.BucketInstanceName unset"))
	}
	return br, nil
}

func (n *NodeClient) getB(ctx context.Context, bName string) (*v1alpha1.Bucket, error) {
	klog.Infof("getting bucket %q", bName)
	// is BucketInstanceName the correct field, or should it be BucketClass
	bkt, err := n.cosiClient.Buckets().Get(ctx, bName, metav1.GetOptions{})
	if err != nil {
		return nil, logErr(getError("bucket", bName, err))
	}
	// TODO: need to enable validation after resolving status issue - Krish
	if bkt == nil {
		return nil, logErr(fmt.Errorf("bucket is nil %q", fmt.Sprintf("%s", bName)))
	}
	if !bkt.Status.BucketAvailable {
		return nil, logErr(fmt.Errorf("bucket is not available yet %q", fmt.Sprintf("%s", bName)))
	}
	return bkt, nil
}

func (n *NodeClient) GetResources(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, secret *v1.Secret, err error) {
	var (
		bar *v1alpha1.BucketAccessRequest
		ba  *v1alpha1.BucketAccess
		br  *v1alpha1.BucketRequest
	)

	if bar, err = n.getBAR(ctx, barName, barNs); err != nil {
		return
	}

	if ba, err = n.getBA(ctx, bar.Spec.BucketAccessName); err != nil {
		return
	}

	if br, err = n.getBR(ctx, bar.Spec.BucketRequestName, barNs); err != nil {
		return
	}

	if bkt, err = n.getB(ctx, br.Spec.BucketInstanceName); err != nil {
		return
	}

	if secret, err = n.kubeClient.CoreV1().Secrets(barNs).Get(ctx, ba.Spec.MintedSecretName, metav1.GetOptions{}); err != nil {
		_ = logErr(getError("secret", fmt.Sprintf("%s/%s", barNs, ba.Spec.MintedSecretName), err))
		return
	}
	return
}

func (n *NodeClient) getProtocol(bkt *v1alpha1.Bucket) (data []byte, err error) {
	klog.Infof("bucket protocol %+v", bkt.Spec.Protocol)
	var protocolConnection interface{}
	switch bkt.Spec.Protocol.Name {
	case v1alpha1.ProtocolNameS3:
		protocolConnection = bkt.Spec.Protocol.S3
	case v1alpha1.ProtocolNameAzure:
		protocolConnection = bkt.Spec.Protocol.AzureBlob
	case v1alpha1.ProtocolNameGCS:
		protocolConnection = bkt.Spec.Protocol.GCS
	default:
		err = fmt.Errorf("unrecognized protocol %q, unable to extract connection data", bkt.Spec.Protocol.Name)
	}
	if err != nil {
		return nil, logErr(err)
	}
	if data, err = json.Marshal(protocolConnection); err != nil {
		return nil, logErr(err)
	}
	return data, nil
}
