package client

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/container-object-storage-interface-api/apis/objectstorage.k8s.io/v1alpha1"
	cosifake "sigs.k8s.io/container-object-storage-interface-api/clientset/fake"
	cs "sigs.k8s.io/container-object-storage-interface-api/clientset/typed/objectstorage.k8s.io/v1alpha1"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util"
)

var (
	ctx = context.Background()
)

const (
	namespace = "test"
)

func getBAR() *v1alpha1.BucketAccessRequest {
	return &v1alpha1.BucketAccessRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
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

func getBA() *v1alpha1.BucketAccess {
	return &v1alpha1.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bucketAccessName",
		},
		Spec: v1alpha1.BucketAccessSpec{
			BucketName: "bucketName",
			BucketAccessRequest: &corev1.ObjectReference{
				Name:      "bucketAccessRequest",
				Namespace: namespace,
			},
			ServiceAccount: &corev1.ObjectReference{
				Name:      "serviceAccount",
				Namespace: namespace,
			},
			PolicyActionsConfigMapData: "policyActionData",
		},
		Status: v1alpha1.BucketAccessStatus{
			AccessGranted: true,
			MintedSecret: &corev1.SecretReference{
				Name:      "mintedSecretName",
				Namespace: namespace,
			},
			AccountID: "accountId",
		},
	}
}

func getSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mintedSecretName",
			Namespace: namespace,
		},
		Immutable: nil,
		Data: map[string][]byte{
			"credentials": []byte("test"),
		},
		Type: corev1.SecretTypeOpaque,
	}
}

func getBR() *v1alpha1.BucketRequest {
	return &v1alpha1.BucketRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
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

func getB() *v1alpha1.Bucket {
	return &v1alpha1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bucketName",
		},
		Spec: v1alpha1.BucketSpec{
			Provisioner:     "test-provisioner",
			BucketClassName: "bucketClassName",
			BucketRequest: &corev1.ObjectReference{
				Name:      "bucketRequestName",
				Namespace: namespace,
			},
		},
		Status: v1alpha1.BucketStatus{
			BucketAvailable: true,
			BucketID:        "bucketId",
		},
	}
}

func TestGetBAR(t *testing.T) {
	type args struct {
		prepare func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface)
		barName string
		barNs   string
	}

	type want struct {
		bar *v1alpha1.BucketAccessRequest
		err error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, getBAR(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				bar: getBAR(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, getBAR(), metav1.CreateOptions{})
				},
				barName: "wrongName",
				barNs:   namespace,
			},
			want: want{
				bar: nil,
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccessrequests.objectstorage.k8s.io", "wrongName"), util.WrapErrorGetBARFailed),
			},
		},
		"FailNoAccess": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					bar := getBAR()
					bar.Status.AccessGranted = false
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, bar, metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				bar: nil,
				err: util.ErrorBARNoAccess,
			},
		},
		"FailNoBAName": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					bar := getBAR()
					bar.Status.BucketAccessName = ""
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, bar, metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				bar: nil,
				err: util.ErrorBARUnsetBA,
			},
		},
		"FailNoBRName": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					bar := getBAR()
					bar.Spec.BucketRequestName = ""
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, bar, metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				bar: nil,
				err: util.ErrorBARUnsetBR,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			bar, err := nc.GetBAR(ctx, tc.barName, tc.barNs)

			if diff := cmp.Diff(tc.want.bar, bar); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}

func TestGetBA(t *testing.T) {
	type args struct {
		prepare func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface)
		baName  string
	}

	type want struct {
		ba  *v1alpha1.BucketAccess
		err error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccesses().Create(ctx, getBA(), metav1.CreateOptions{})
				},
				baName: "bucketAccessName",
			},
			want: want{
				ba:  getBA(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccesses().Create(ctx, getBA(), metav1.CreateOptions{})
				},
				baName: "wrongName",
			},
			want: want{
				ba:  nil,
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccesses.objectstorage.k8s.io", "wrongName"), util.WrapErrorGetBAFailed),
			},
		},
		"FailNoAccess": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					ba := getBA()
					ba.Status.AccessGranted = false
					_, _ = cosi.BucketAccesses().Create(ctx, ba, metav1.CreateOptions{})
				},
				baName: "bucketAccessName",
			},
			want: want{
				ba:  nil,
				err: util.ErrorBANoAccess,
			},
		},
		"FailNoMintedSecretRef": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					ba := getBA()
					ba.Status.MintedSecret = nil
					_, _ = cosi.BucketAccesses().Create(ctx, ba, metav1.CreateOptions{})
				},
				baName: "bucketAccessName",
			},
			want: want{
				ba:  nil,
				err: util.ErrorBANoMintedSecret,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			ba, err := nc.GetBA(ctx, tc.baName)

			if diff := cmp.Diff(tc.want.ba, ba); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}

func TestGetBR(t *testing.T) {
	type args struct {
		prepare func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface)
		brName  string
		brNs    string
	}

	type want struct {
		br  *v1alpha1.BucketRequest
		err error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketRequests(namespace).Create(ctx, getBR(), metav1.CreateOptions{})
				},
				brName: "bucketRequestName",
				brNs:   namespace,
			},
			want: want{
				br:  getBR(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketRequests(namespace).Create(ctx, getBR(), metav1.CreateOptions{})
				},
				brName: "wrongName",
				brNs:   namespace,
			},
			want: want{
				br:  nil,
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketrequests.objectstorage.k8s.io", "wrongName"), util.WrapErrorGetBRFailed),
			},
		},
		"FailNotAvailable": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					br := getBR()
					br.Status.BucketAvailable = false
					_, _ = cosi.BucketRequests(namespace).Create(ctx, br, metav1.CreateOptions{})
				},
				brName: "bucketRequestName",
				brNs:   namespace,
			},
			want: want{
				br:  nil,
				err: util.ErrorBRNotAvailable,
			},
		},
		"FailNoBName": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					br := getBR()
					br.Status.BucketName = ""
					_, _ = cosi.BucketRequests(namespace).Create(ctx, br, metav1.CreateOptions{})
				},
				brName: "bucketRequestName",
				brNs:   namespace,
			},
			want: want{
				br:  nil,
				err: util.ErrorBRUnsetBucketName,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			br, err := nc.GetBR(ctx, tc.brName, tc.brNs)

			if diff := cmp.Diff(tc.want.br, br); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}

func TestGetB(t *testing.T) {
	type args struct {
		prepare func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface)
		bName   string
	}

	type want struct {
		b   *v1alpha1.Bucket
		err error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, getB(), metav1.CreateOptions{})
				},
				bName: "bucketName",
			},
			want: want{
				b:   getB(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, getB(), metav1.CreateOptions{})
				},
				bName: "wrongName",
			},
			want: want{
				b:   nil,
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "buckets.objectstorage.k8s.io", "wrongName"), util.WrapErrorGetBFailed),
			},
		},
		"FailNotAvailable": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					b := getB()
					b.Status.BucketAvailable = false
					_, _ = cosi.Buckets().Create(ctx, b, metav1.CreateOptions{})
				},
				bName: "bucketName",
			},
			want: want{
				b:   nil,
				err: util.ErrorBNotAvailable,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			b, err := nc.GetB(ctx, tc.bName)

			if diff := cmp.Diff(tc.want.b, b); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}

func TestGetResources(t *testing.T) {
	type args struct {
		prepare func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface)
		barName string
		barNs   string
	}

	type want struct {
		b      *v1alpha1.Bucket
		ba     *v1alpha1.BucketAccess
		secret *corev1.Secret
		err    error
	}

	cases := map[string]struct {
		args
		want
	}{
		"Successful": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, getB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, getBAR(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, getBA(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(namespace).Create(ctx, getSecret(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				b:      getB(),
				ba:     getBA(),
				secret: getSecret(),
				err:    nil,
			},
		},
		"failedMissingBAR": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, getB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, getBA(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(namespace).Create(ctx, getSecret(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccessrequests.objectstorage.k8s.io", "bucketAccessRequestName"), util.WrapErrorGetBARFailed),
			},
		},
		"failedMissingBA": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, getB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, getBAR(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(namespace).Create(ctx, getSecret(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccesses.objectstorage.k8s.io", "bucketAccessName"), util.WrapErrorGetBAFailed),
			},
		},
		"failedMissingB": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, getBAR(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, getBA(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(namespace).Create(ctx, getSecret(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				ba:  getBA(),
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "buckets.objectstorage.k8s.io", "bucketName"), util.WrapErrorGetBFailed),
			},
		},
		"failedMissingSecret": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, getB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccessRequests(namespace).Create(ctx, getBAR(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, getBA(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   namespace,
			},
			want: want{
				b:   getB(),
				ba:  getBA(),
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "secrets", "mintedSecretName"), util.WrapErrorGetSecretFailed),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			b, ba, secret, err := nc.GetResources(ctx, tc.barName, tc.barNs)

			if diff := cmp.Diff(tc.want.b, b); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.ba, ba); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.secret, secret); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}

func TestGetProtocol(t *testing.T) {
	type args struct {
		prepare func(bkt *v1alpha1.Bucket) *v1alpha1.Bucket
	}

	type want struct {
		data string
		err  error
	}

	cases := map[string]struct {
		args
		want
	}{
		"SuccessfulS3": {
			args: args{
				prepare: func(bkt *v1alpha1.Bucket) *v1alpha1.Bucket {
					bkt.Spec.Protocol = v1alpha1.Protocol{
						S3: &v1alpha1.S3Protocol{
							Endpoint:         "endpoint",
							BucketName:       "bucketName",
							Region:           "region",
							SignatureVersion: "signatureVersion",
						},
					}
					return bkt
				},
			},
			want: want{
				data: `{"endpoint":"endpoint", "bucketName":"bucketName", "region":"region", "signatureVersion":"signatureVersion"}`,
				err:  nil,
			},
		},
		"SuccessfulGCP": {
			args: args{
				prepare: func(bkt *v1alpha1.Bucket) *v1alpha1.Bucket {
					bkt.Spec.Protocol = v1alpha1.Protocol{
						GCS: &v1alpha1.GCSProtocol{
							BucketName:     "bucketName",
							PrivateKeyName: "privateKeyName",
							ProjectID:      "projectID",
							ServiceAccount: "serviceAccount",
						},
					}
					return bkt
				},
			},
			want: want{
				data: `{"bucketName":"bucketName", "privateKeyName":"privateKeyName", "projectID":"projectID", "serviceAccount":"serviceAccount"}`,
				err:  nil,
			},
		},
		"SuccessfulAzure": {
			args: args{
				prepare: func(bkt *v1alpha1.Bucket) *v1alpha1.Bucket {
					bkt.Spec.Protocol = v1alpha1.Protocol{
						AzureBlob: &v1alpha1.AzureProtocol{
							ContainerName:  "containerName",
							StorageAccount: "storageAccount",
						},
					}
					return bkt
				},
			},
			want: want{
				data: `{"containerName":"containerName", "storageAccount":"storageAccount"}`,
				err:  nil,
			},
		},
		"FailMissingProtocol": {
			args: args{
				prepare: func(bkt *v1alpha1.Bucket) *v1alpha1.Bucket {
					bkt.Spec.Protocol = v1alpha1.Protocol{}
					return bkt
				},
			},
			want: want{
				err: util.ErrorInvalidProtocol,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
			}

			bkt := getB()

			data, err := nc.GetProtocol(tc.prepare(bkt))

			var wantData interface{}
			var haveData interface{}

			_ = json.Unmarshal(data, &haveData)
			_ = json.Unmarshal([]byte(tc.data), &wantData)

			if diff := cmp.Diff(wantData, haveData); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}

			if diff := cmp.Diff(tc.want.err, err, util.EquateErrors()); diff != "" {
				t.Errorf("r: -want, +got:\n%s", diff)
			}
		})
	}
}
