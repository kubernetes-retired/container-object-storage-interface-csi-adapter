package fake

import (
	"context"
	"k8s.io/client-go/tools/record"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
)

var _ client.NodeClient = &FakeNodeClient{}

type FakeNodeClient struct {
	MockGetBAR func(ctx context.Context, pod *v1.Pod, barName, barNs string) (*v1alpha1.BucketAccessRequest, error)
	MockGetBA  func(ctx context.Context, pod *v1.Pod, baName string) (*v1alpha1.BucketAccess, error)
	MockGetBR  func(ctx context.Context, pod *v1.Pod, brName, brNs string) (*v1alpha1.BucketRequest, error)
	MockGetB   func(ctx context.Context, pod *v1.Pod, bName string) (*v1alpha1.Bucket, error)
	MockGetPod func(ctx context.Context, podName, podNs string) (*v1.Pod, error)

	MockGetResources func(ctx context.Context, barName, podName, podNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, pod *v1.Pod, err error)

	MockAddBAFinalizer    func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
	MockRemoveBAFinalizer func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
}

func (f FakeNodeClient) GetPod(ctx context.Context, podName, podNs string) (*v1.Pod, error) {
	return f.MockGetPod(ctx, podName, podNs)
}

var fRecorder = record.NewFakeRecorder(10)

func (f FakeNodeClient) Recorder() record.EventRecorder {
	return fRecorder
}

func (f FakeNodeClient) GetBAR(ctx context.Context, pod *v1.Pod, barName, barNs string) (*v1alpha1.BucketAccessRequest, error) {
	return f.MockGetBAR(ctx, pod, barName, barNs)
}

func (f FakeNodeClient) GetBA(ctx context.Context, pod *v1.Pod, baName string) (*v1alpha1.BucketAccess, error) {
	return f.MockGetBA(ctx, pod, baName)
}

func (f FakeNodeClient) GetBR(ctx context.Context, pod *v1.Pod, brName, brNs string) (*v1alpha1.BucketRequest, error) {
	return f.MockGetBR(ctx, pod, brName, brNs)
}

func (f FakeNodeClient) GetB(ctx context.Context, pod *v1.Pod, bName string) (*v1alpha1.Bucket, error) {
	return f.MockGetB(ctx, pod, bName)
}

func (f FakeNodeClient) GetResources(ctx context.Context, barName, podName, podNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, pod *v1.Pod, err error) {
	return f.MockGetResources(ctx, barName, podName, podNs)
}

func (f FakeNodeClient) AddBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
	return f.MockAddBAFinalizer(ctx, ba, BAFinalizer)
}

func (f FakeNodeClient) RemoveBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
	return f.MockRemoveBAFinalizer(ctx, ba, BAFinalizer)
}
