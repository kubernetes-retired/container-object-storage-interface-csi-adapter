package util

import "errors"

const (
	WrapErrorGetBARFailed = "get bucketAccessRequest failed"
	WrapErrorGetBAFailed  = "get bucketAccess failed"
	WrapErrorGetBRFailed  = "get bucketRequest failed"
	WrapErrorGetBFailed   = "get bucket failed"

	WrapErrorGetSecretFailed = "failed to get minted secret from bucketAccess"

	WrapErrorMarshalProtocolFailed = "failed to marshal bucket protocol"

	WrapErrorMkdirFailed              = "failed to mkdir for bucketPath on publish"
	WrapErrorFailedToCreateVolumeFile = "failed to create file in ephemeral volume"
	WrapErrorFailedToCreateBucketFile = "failed to create file in bucket mount folder"

	WrapErrorFailedRemoveDirectory    = "failed to remove directory after error"
	WrapErrorFailedToParseSecret      = "failed to parse secret"
	WrapErrorFailedToWriteProtocol    = "failed to write protocolConnection to mount volume"
	WrapErrorFailedToWriteCredentials = "failed to write credentials to mount volume"
	WrapErrorFailedToMountVolume      = "failed to mount ephemeral volume to pod"

	WrapErrorFailedToAddFinalizer    = "failed to add finalizer to bucketAccess"
	WrapErrorFailedToMarshalMetadata = "failed to marshal Metadata struct"
	WrapErrorFailedToWriteMetadata   = "failed to write metadata to disk"
	WrapErrorFailedToMkdirForMount   = "failed to mkdir when mounting bucket"

	WrapErrorFailedToReadMetadataFile  = "failed to read metadata file from volume"
	WrapErrorFailedToUnmarshalMetadata = "failed unable to unmarshal metadata from volume"
	WrapErrorFailedToRemoveFinalizer   = "failed to remove finalizer from bucketAccess"
	WrapErrorFailedToUnmountVolume     = "failed to unmount and clean volume"
	WrapErrorFailedToRemoveDir         = "failed to remove directory"

	WrapErrorCreatingFile  = "error when creating file"
	WrapErrorWritingToFile = "error when writing file"
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

var (
	ErrorTemplateVolCtxUnset          = "required volume context key unset: %v"
	ErrorTemplateVolumeAlreadyMounted = "%s is already mounted"
	ErrorTemplateMountFailed          = "failed to mount device: %s at %s"
)
