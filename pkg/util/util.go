package util

import (
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func ParseData(s *v1.Secret) ([]byte, error) {
	output := make(map[string]string)
	for key, value := range s.Data {
		output[key] = string(value)
	}
	data, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func ParseValue(key string, volCtx map[string]string) (string, error) {
	value, ok := volCtx[key]
	if !ok {
		return "", fmt.Errorf(ErrorTemplateVolCtxUnset, key)
	}
	return value, nil
}

// logErr should be called at the interface method scope, prior to returning errors to the gRPC client.
func LogErr(e error) error {
	klog.Error(e)
	return e
}
