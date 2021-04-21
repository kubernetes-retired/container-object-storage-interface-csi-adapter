package fake

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/client"
)

var _ client.NodeClient = &FakeNodeClient{}

type FakeNodeClient struct {
	MockGetBAR func(ctx context.Context, barName, barNs string) (*v1alpha1.BucketAccessRequest, error)
	MockGetBA func(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error)
	MockGetBR func(ctx context.Context, brName, brNs string) (*v1alpha1.BucketRequest, error)
	MockGetB func(ctx context.Context, bName string) (*v1alpha1.Bucket, error)

	MockGetResources func(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error)
	MockGetProtocol func(bkt *v1alpha1.Bucket) (data []byte, err error)

	MockAddBAFinalizer func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
	MockRemoveBAFinalizer func(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error
}

func (f FakeNodeClient) GetBAR(ctx context.Context, barName, barNs string) (*v1alpha1.BucketAccessRequest, error) {
	return f.GetBAR(ctx, barName, barNs)
}

func (f FakeNodeClient) GetBA(ctx context.Context, baName string) (*v1alpha1.BucketAccess, error) {
	return f.GetBA(ctx, baName)
}

func (f FakeNodeClient) GetBR(ctx context.Context, brName, brNs string) (*v1alpha1.BucketRequest, error) {
	return f.GetBR(ctx, brName, brNs)
}

func (f FakeNodeClient) GetB(ctx context.Context, bName string) (*v1alpha1.Bucket, error) {
	return f.GetB(ctx, bName)
}

func (f FakeNodeClient) GetResources(ctx context.Context, barName, barNs string) (bkt *v1alpha1.Bucket, ba *v1alpha1.BucketAccess, secret *v1.Secret, err error) {
	return f.GetResources(ctx, barName, barNs)
}

func (f FakeNodeClient) GetProtocol(bkt *v1alpha1.Bucket) (data []byte, err error) {
	return f.GetProtocol(bkt)
}

func (f FakeNodeClient) AddBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
	return f.AddBAFinalizer(ctx, ba, BAFinalizer)
}

func (f FakeNodeClient) RemoveBAFinalizer(ctx context.Context, ba *v1alpha1.BucketAccess, BAFinalizer string) error {
	return f.RemoveBAFinalizer(ctx, ba, BAFinalizer)
}
