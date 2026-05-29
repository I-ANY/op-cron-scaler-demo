/*
Copyright 2026.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	MinuteTimeLayout = "15:04"
	SUCCESS          = "Success"
	FAILED           = "Failed"
	PENDING          = "Pending"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type DeploymentInfo struct {
	Replicas  int32  `json:"replicas"`
	NameSpace string `json:"namespace"`
	Name      string `json:"name"`
}

// CronScaleDemoSpec defines the desired state of CronScaleDemo
type CronScaleDemoSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of CronScaleDemo. Edit cronscaledemo_types.go to remove/update
	// +optional

	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	// +kubebuilder:validation:Required
	StartTime string `json:"startTime"`
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	// +kubebuilder:validation:Required
	EndTime string `json:"endTime"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:validation:Required
	Replicas        int32                   `json:"replicas,required"`
	DefaultReplicas int32                   `json:"defaultReplicas"`
	Deployments     []DeploymentScaleTarget `json:"deployments,required"`
}

type DeploymentScaleTarget struct {
	Name      string `json:"name"`
	NameSpace string `json:"namespace"`
}

// CronScaleDemoStatus defines the observed state of CronScaleDemo.
type CronScaleDemoStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the CronScaleDemo resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	Status     string             `json:"status"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// 给 status增加一个标记，目的是告诉 controller-tools 在生成 CRD 时，给自定义资源增加一列自定义显示列，这样用 kubectl get 时能直接看到这个字段
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// CronScaleDemo is the Schema for the cronscaledemoes API
type CronScaleDemo struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of CronScaleDemo
	// +required
	Spec CronScaleDemoSpec `json:"spec"`

	// status defines the observed state of CronScaleDemo
	// +optional
	Status CronScaleDemoStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// CronScaleDemoList contains a list of CronScaleDemo
type CronScaleDemoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CronScaleDemo `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CronScaleDemo{}, &CronScaleDemoList{})
}
