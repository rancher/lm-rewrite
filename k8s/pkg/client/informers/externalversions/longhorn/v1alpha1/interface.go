/*
Copyright 2018 The Kubernetes Authors.

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

// This file was automatically generated by informer-gen

package v1alpha1

import (
	internalinterfaces "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/internalinterfaces"
)

// Interface provides access to all the informers in this group version.
type Interface interface {
	// Engines returns a EngineInformer.
	Engines() EngineInformer
	// Replicas returns a ReplicaInformer.
	Replicas() ReplicaInformer
	// Settings returns a SettingInformer.
	Settings() SettingInformer
	// Volumes returns a VolumeInformer.
	Volumes() VolumeInformer
}

type version struct {
	internalinterfaces.SharedInformerFactory
}

// New returns a new Interface.
func New(f internalinterfaces.SharedInformerFactory) Interface {
	return &version{f}
}

// Engines returns a EngineInformer.
func (v *version) Engines() EngineInformer {
	return &engineInformer{factory: v.SharedInformerFactory}
}

// Replicas returns a ReplicaInformer.
func (v *version) Replicas() ReplicaInformer {
	return &replicaInformer{factory: v.SharedInformerFactory}
}

// Settings returns a SettingInformer.
func (v *version) Settings() SettingInformer {
	return &settingInformer{factory: v.SharedInformerFactory}
}

// Volumes returns a VolumeInformer.
func (v *version) Volumes() VolumeInformer {
	return &volumeInformer{factory: v.SharedInformerFactory}
}
