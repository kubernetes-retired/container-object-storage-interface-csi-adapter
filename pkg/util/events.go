package util

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

const (
	BARNotReady = "BARNotReady"
	BANotReady  = "BANotReady"
	BRNotReady  = "BRNotReady"
	BNotReady   = "BNotReady"

	ResourcesReady     = "ResourceReady"
	WritingCredentials = "WritingCredentials"
	SuccessfulPublish  = "Success"
)

var (
	BARBucketRequestNotSet = EventResource{
		reason:  BARNotReady,
		message: "Bucket Access Request spec field bucketRequestName is not set",
	}
	BARAccessNotGranted = EventResource{
		reason:  BARNotReady,
		message: "Bucket Access Request has not been granted access yet",
	}
	BARBucketAccessNotSet = EventResource{
		reason:  BARNotReady,
		message: "Bucket Access Request status field bucketAccessName is not set",
	}

	BAAccessNotGranted = EventResource{
		reason:  BANotReady,
		message: "Bucket Access has not been granted access yet",
	}
	BAMintedSecretNotSet = EventResource{
		reason:  BANotReady,
		message: "Bucket Access does not have reference to minted secret",
	}

	BRNotAvailable = EventResource{
		reason:  BRNotReady,
		message: "Bucket Request is not available yet",
	}
	BRBucketNameNotSet = EventResource{
		reason:  BRNotReady,
		message: "Bucket Request status field bucketName is not set",
	}

	BNotAvailable = EventResource{
		reason:  BNotReady,
		message: "Bucket is not available yet",
	}

	MintedSecretNotFound = EventResource{
		reason:  BANotReady,
		message: "Minted credentials secret not found",
	}
)

var (
	AllResourcesReady = EventResource{
		reason:  ResourcesReady,
		message: "Bucket resources already found and ready",
	}

	CredentialsWritten = EventResource{
		reason:  WritingCredentials,
		message: "All connection information written to volume mount",
	}

	SuccessfullyPublishedVolume = EventResource{
		reason:  SuccessfulPublish,
		message: "Publish credentials completed successfully",
	}

	SuccessfullyUnpublishedVolume = EventResource{
		reason:  SuccessfulPublish,
		message: "Volume successfully unpublished from pod",
	}
)

type EventResource struct {
	reason  string
	message string
}

func EmitWarningEvent(recorder record.EventRecorder, object runtime.Object, resource EventResource) {
	recorder.Event(object, corev1.EventTypeWarning, resource.reason, resource.message)
}

func EmitNormalEvent(recorder record.EventRecorder, object runtime.Object, resource EventResource) {
	recorder.Event(object, corev1.EventTypeNormal, resource.reason, resource.message)
}
