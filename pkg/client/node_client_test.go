package client

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/client-go/tools/record"
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
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/util/test"
)

var (
	ctx = context.Background()
)

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
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, testutils.GetBAR(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				bar: testutils.GetBAR(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, testutils.GetBAR(), metav1.CreateOptions{})
				},
				barName: "wrongName",
				barNs:   testutils.Namespace,
			},
			want: want{
				bar: nil,
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccessrequests.objectstorage.k8s.io", "wrongName"), util.WrapErrorGetBARFailed),
			},
		},
		"FailNoAccess": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					bar := testutils.GetBAR()
					bar.Status.AccessGranted = false
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, bar, metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				bar: nil,
				err: util.ErrorBARNoAccess,
			},
		},
		"FailNoBAName": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					bar := testutils.GetBAR()
					bar.Status.BucketAccessName = ""
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, bar, metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				bar: nil,
				err: util.ErrorBARUnsetBA,
			},
		},
		"FailNoBRName": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					bar := testutils.GetBAR()
					bar.Spec.BucketRequestName = ""
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, bar, metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
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
				recorder:   record.NewFakeRecorder(10),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			bar, err := nc.GetBAR(ctx, testutils.GetPod(), tc.barName, tc.barNs)

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
					_, _ = cosi.BucketAccesses().Create(ctx, testutils.GetBA(), metav1.CreateOptions{})
				},
				baName: "bucketAccessName",
			},
			want: want{
				ba:  testutils.GetBA(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccesses().Create(ctx, testutils.GetBA(), metav1.CreateOptions{})
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
					ba := testutils.GetBA()
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
					ba := testutils.GetBA()
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
				recorder:   record.NewFakeRecorder(10),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			ba, err := nc.GetBA(ctx, testutils.GetPod(), tc.baName)

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
					_, _ = cosi.BucketRequests(testutils.Namespace).Create(ctx, testutils.GetBR(), metav1.CreateOptions{})
				},
				brName: "bucketRequestName",
				brNs:   testutils.Namespace,
			},
			want: want{
				br:  testutils.GetBR(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketRequests(testutils.Namespace).Create(ctx, testutils.GetBR(), metav1.CreateOptions{})
				},
				brName: "wrongName",
				brNs:   testutils.Namespace,
			},
			want: want{
				br:  nil,
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketrequests.objectstorage.k8s.io", "wrongName"), util.WrapErrorGetBRFailed),
			},
		},
		"FailNotAvailable": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					br := testutils.GetBR()
					br.Status.BucketAvailable = false
					_, _ = cosi.BucketRequests(testutils.Namespace).Create(ctx, br, metav1.CreateOptions{})
				},
				brName: "bucketRequestName",
				brNs:   testutils.Namespace,
			},
			want: want{
				br:  nil,
				err: util.ErrorBRNotAvailable,
			},
		},
		"FailNoBName": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					br := testutils.GetBR()
					br.Status.BucketName = ""
					_, _ = cosi.BucketRequests(testutils.Namespace).Create(ctx, br, metav1.CreateOptions{})
				},
				brName: "bucketRequestName",
				brNs:   testutils.Namespace,
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
				recorder:   record.NewFakeRecorder(10),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			br, err := nc.GetBR(ctx, testutils.GetPod(), tc.brName, tc.brNs)

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
					_, _ = cosi.Buckets().Create(ctx, testutils.GetB(), metav1.CreateOptions{})
				},
				bName: "bucketName",
			},
			want: want{
				b:   testutils.GetB(),
				err: nil,
			},
		},
		"NotFound": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, testutils.GetB(), metav1.CreateOptions{})
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
					b := testutils.GetB()
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
				recorder:   record.NewFakeRecorder(10),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			b, err := nc.GetB(ctx, testutils.GetPod(), tc.bName)

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
					_, _ = cosi.Buckets().Create(ctx, testutils.GetB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, testutils.GetBAR(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, testutils.GetBA(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(testutils.Namespace).Create(ctx, testutils.GetSecret(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Pods(testutils.Namespace).Create(ctx, testutils.GetPod(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				b:      testutils.GetB(),
				ba:     testutils.GetBA(),
				secret: testutils.GetSecret(),
				err:    nil,
			},
		},
		"failedMissingBAR": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, testutils.GetB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, testutils.GetBA(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(testutils.Namespace).Create(ctx, testutils.GetSecret(), metav1.CreateOptions{})
					_, _ = cs.CoreV1().Pods(testutils.Namespace).Create(ctx, testutils.GetPod(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccessrequests.objectstorage.k8s.io", "bucketAccessRequestName"), util.WrapErrorGetBARFailed),
			},
		},
		"failedMissingBA": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, testutils.GetB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, testutils.GetBAR(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(testutils.Namespace).Create(ctx, testutils.GetSecret(), metav1.CreateOptions{})
					_, _ = cs.CoreV1().Pods(testutils.Namespace).Create(ctx, testutils.GetPod(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "bucketaccesses.objectstorage.k8s.io", "bucketAccessName"), util.WrapErrorGetBAFailed),
			},
		},
		"failedMissingB": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, testutils.GetBAR(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, testutils.GetBA(), metav1.CreateOptions{})

					_, _ = cs.CoreV1().Secrets(testutils.Namespace).Create(ctx, testutils.GetSecret(), metav1.CreateOptions{})
					_, _ = cs.CoreV1().Pods(testutils.Namespace).Create(ctx, testutils.GetPod(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				ba:  testutils.GetBA(),
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "buckets.objectstorage.k8s.io", "bucketName"), util.WrapErrorGetBFailed),
			},
		},
		"failedMissingSecret": {
			args: args{
				prepare: func(cs kubernetes.Interface, cosi cs.ObjectstorageV1alpha1Interface) {
					_, _ = cosi.Buckets().Create(ctx, testutils.GetB(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccessRequests(testutils.Namespace).Create(ctx, testutils.GetBAR(), metav1.CreateOptions{})
					_, _ = cosi.BucketAccesses().Create(ctx, testutils.GetBA(), metav1.CreateOptions{})
					_, _ = cs.CoreV1().Pods(testutils.Namespace).Create(ctx, testutils.GetPod(), metav1.CreateOptions{})
				},
				barName: "bucketAccessRequestName",
				barNs:   testutils.Namespace,
			},
			want: want{
				b:   testutils.GetB(),
				ba:  testutils.GetBA(),
				err: errors.Wrap(fmt.Errorf("%s \"%s\" not found", "secrets", "mintedSecretName"), util.WrapErrorGetSecretFailed),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			nc := &nodeClient{
				kubeClient: k8sfake.NewSimpleClientset(),
				cosiClient: cosifake.NewSimpleClientset().ObjectstorageV1alpha1(),
				recorder:   record.NewFakeRecorder(10),
			}

			tc.prepare(nc.kubeClient, nc.cosiClient)

			b, ba, secret, _, err := nc.GetResources(ctx, tc.barName, "podName", testutils.Namespace)

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

			bkt := testutils.GetB()

			data, err := GetProtocol(tc.prepare(bkt))

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
