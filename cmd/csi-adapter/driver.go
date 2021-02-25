/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"

	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"k8s.io/klog/v2"

	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/controller"
	id "sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/identity"
	"sigs.k8s.io/container-object-storage-interface-csi-adapter/pkg/node"
)

func driver(args []string) error {
	if protocol == "unix" {
		if err := os.RemoveAll(listen); err != nil {
			klog.Fatalf("could not prepare socket: %v", err)
		}
	}

	idServer, err := id.NewIdentityServer(identity, Version, map[string]string{})
	if err != nil {
		return err
	}
	klog.InfoS("identity server prepared")

	nodeServer := node.NewNodeServerOrDie(identity, nodeID, dataRoot, volumeLimit)
	controllerServer, err := controller.NewControllerServer()

	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(listen, idServer, controllerServer, nodeServer)
	s.Wait()

	return nil
}
