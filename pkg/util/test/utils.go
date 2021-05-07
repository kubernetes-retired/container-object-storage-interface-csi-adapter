package testutils

import (
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
)

const (
	Namespace = "test"
)

func GetPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "podName",
			Namespace: Namespace,
		},
	}
}

func GetBAR() *v1alpha1.BucketAccessRequest {
	return &v1alpha1.BucketAccessRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: Namespace,
			Name:      "bucketAccessRequestName",
		},
		Spec: v1alpha1.BucketAccessRequestSpec{
			BucketName:            "bucketName",
			BucketRequestName:     "bucketRequestName",
			BucketAccessClassName: "bucketAccessClassName",
			ServiceAccountName:    "serviceAccountName",
		},
		Status: v1alpha1.BucketAccessRequestStatus{
			AccessGranted:    true,
			BucketAccessName: "bucketAccessName",
		},
	}
}

func GetBA() *v1alpha1.BucketAccess {
	return &v1alpha1.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bucketAccessName",
		},
		Spec: v1alpha1.BucketAccessSpec{
			BucketName: "bucketName",
			BucketAccessRequest: &corev1.ObjectReference{
				Name:      "bucketAccessRequest",
				Namespace: Namespace,
			},
			ServiceAccount: &corev1.ObjectReference{
				Name:      "serviceAccount",
				Namespace: Namespace,
			},
			PolicyActionsConfigMapData: "policyActionData",
		},
		Status: v1alpha1.BucketAccessStatus{
			AccessGranted: true,
			MintedSecret: &corev1.SecretReference{
				Name:      "mintedSecretName",
				Namespace: Namespace,
			},
			AccountID: "accountId",
		},
	}
}

func GetSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mintedSecretName",
			Namespace: Namespace,
		},
		Immutable: nil,
		Data: map[string][]byte{
			"credentials": []byte("test"),
		},
		Type: corev1.SecretTypeOpaque,
	}
}

func GetBR() *v1alpha1.BucketRequest {
	return &v1alpha1.BucketRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: Namespace,
			Name:      "bucketRequestName",
		},
		Spec: v1alpha1.BucketRequestSpec{
			BucketClassName: "bucketClassName",
		},
		Status: v1alpha1.BucketRequestStatus{
			BucketAvailable: true,
			BucketName:      "bucketName",
		},
	}
}

func GetB(mod ...BktModifier) *v1alpha1.Bucket {
	bkt := &v1alpha1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bucketName",
		},
		Spec: v1alpha1.BucketSpec{
			Provisioner:     "test-provisioner",
			BucketClassName: "bucketClassName",
			BucketRequest: &corev1.ObjectReference{
				Name:      "bucketRequestName",
				Namespace: Namespace,
			},
			Protocol: v1alpha1.Protocol{
				S3: &v1alpha1.S3Protocol{
					Endpoint:         "endpoint",
					BucketName:       "bucketName",
					Region:           "region",
					SignatureVersion: "signatureVersion",
				},
			},
		},
		Status: v1alpha1.BucketStatus{
			BucketAvailable: true,
			BucketID:        "bucketId",
		},
	}

	for _, m := range mod {
		m(bkt)
	}

	return bkt
}

type BktModifier func(bkt *v1alpha1.Bucket)

func WithProtocol(proto v1alpha1.Protocol) BktModifier {
	return func(bkt *v1alpha1.Bucket) {
		bkt.Spec.Protocol = proto
	}
}

func MultipleWrap(err error, wrappers ...string) error {
	var te error
	for _, v := range wrappers {
		te = errors.Wrap(err, v)
		err = te
	}
	return te
}
