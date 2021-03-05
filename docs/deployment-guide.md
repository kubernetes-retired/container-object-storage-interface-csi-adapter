# Deploying Container Object Storage Interface CSI Adapter On Kubernetes

This document describes steps for Kubernetes administrators to setup Container Object Storage Interface (COSI) CSI Adapter onto a Kubernetes cluster.

COSI CSI Adapter can be setup using the [kustomization file](https://github.com/kubernetes-sigs/container-object-storage-interface-csi-adapter/blob/master/kustomization.yaml) from the [container-object-storage-interface-csi-adapter](https://github.com/kubernetes-sigs/container-object-storage-interface-csi-adapter) repository with following command:

```sh
  kubectl create -k github.com/kubernetes-sigs/container-object-storage-interface-csi-adapter
```
The output should look like the following:
```sh
storageclass.storage.k8s.io/objectstorage.k8s.io created
serviceaccount/objectstorage-csi-adapter-sa created
role.rbac.authorization.k8s.io/objectstorage-csi-adapter created
clusterrole.rbac.authorization.k8s.io/objectstorage-csi-adapter-role created
rolebinding.rbac.authorization.k8s.io/objectstorage-csi-adapter created
clusterrolebinding.rbac.authorization.k8s.io/objectstorage-csi-adapter created
secret/objectstorage.k8s.io created
daemonset.apps/objectstorage-csi-adapter created
csidriver.storage.k8s.io/objectstorage.k8s.io created
```

The CSI Adapter will be deployed in the `default` namespace.

