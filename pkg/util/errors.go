package util

import "errors"

const (
	WrapErrorGetBARFailed = "get bucketAccessRequest failed"
	WrapErrorGetBAFailed  = "get bucketAccess failed"
	WrapErrorGetBRFailed  = "get bucketRequest failed"
	WrapErrorGetBFailed   = "get bucket failed"

	WrapErrorGetSecretFailed = "failed to get minted secret from bucketAccess"

	WrapErrorMarshalProtocolFailed = "failed to marshal bucket protocol"
)

var (
	ErrorBARNoAccess = errors.New("bucketAccessRequest does not grant access")
	ErrorBARUnsetBR  = errors.New("bucketAccessRequest.Spec.BucketRequestName unset")
	ErrorBARUnsetBA  = errors.New("bucketAccessRequest.Status.BucketAccessName unset")

	ErrorBANoAccess       = errors.New("bucketAccess does not grant access")
	ErrorBANoMintedSecret = errors.New("bucketAccess.Status.MintedSecretName unset")

	ErrorBRNotAvailable    = errors.New("bucketRequest is not available yet")
	ErrorBRUnsetBucketName = errors.New("bucketRequest.Status.BucketInstanceName unset")

	ErrorBNotAvailable = errors.New("bucket is not available yet")

	ErrorInvalidProtocol = errors.New("unrecognized protocol, unable to extract connection data")
)
