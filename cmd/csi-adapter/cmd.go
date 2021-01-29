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
	"flag"
	"os"

	_ "github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Version string

// flags
var (
	identity    string
	nodeID      string
	protocol    string
	listen      string
	dataRoot    string
	volumeLimit int64
)

var driverCmd = &cobra.Command{
	Use:          os.Args[0],
	Short:        "Ephemeral CSI driver for use in the COSI",
	Long:         "This Container Storage Interface (CSI) driver provides the ability to reference Bucket and BucketAccess objects, extracting connection/credential information and writing it to the Pod's filesystem. This driver does not manage the lifecycle of the bucket or the backing of the objects themselves, it only acts as the middle-man.",
	SilenceUsage: true,
	RunE: func(c *cobra.Command, args []string) error {
		return driver(args)
	},
}

func init() {
	Version = "v0.0.1"

	viper.AutomaticEnv()
	// parse the go default flagset to get flags for glog and other packages in future
	driverCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	// defaulting this to true so that logs are printed to console
	_ = flag.Set("logtostderr", "true")

	driverCmd.PersistentFlags().StringVarP(&identity, "identity", "i", identity, "identity of this COSI CSI driver")
	driverCmd.PersistentFlags().StringVarP(&nodeID, "node-id", "n", nodeID, "identity of the node in which COSI CSI driver is running")
	driverCmd.PersistentFlags().StringVarP(&listen, "listen", "l", listen, "address of the listening socket for the node server")
	driverCmd.PersistentFlags().StringVarP(&protocol, "protocol", "p", protocol, "must be one of tcp, tcp4, tcp6, unix, unixpacket")
	driverCmd.PersistentFlags().StringVarP(&dataRoot, "data-path", "d", protocol, "the path to the directory for storing secrets")
	driverCmd.PersistentFlags().Int64VarP(&volumeLimit, "max-volumes", "m", volumeLimit, "the maximum amount of volumes which can be assigned to a node")

	_ = driverCmd.PersistentFlags().MarkHidden("alsologtostderr")
	_ = driverCmd.PersistentFlags().MarkHidden("log_backtrace_at")
	_ = driverCmd.PersistentFlags().MarkHidden("log_dir")
	_ = driverCmd.PersistentFlags().MarkHidden("logtostderr")
	_ = driverCmd.PersistentFlags().MarkHidden("master")
	_ = driverCmd.PersistentFlags().MarkHidden("stderrthreshold")
	_ = driverCmd.PersistentFlags().MarkHidden("vmodule")

	// suppress the incorrect prefix in glog output
	_ = flag.CommandLine.Parse([]string{})
	_ = viper.BindPFlags(driverCmd.PersistentFlags())
}

func Execute() error {
	return driverCmd.Execute()
}
