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
	RUNNING          = "Running"
	RESTORED         = "Restored"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type DeploymentInfo struct {
	Replicas  int32  `json:"replicas"`
	NameSpace string `json:"namespace"`
	Name      string `json:"name"`
}

// CronScalerSpec defines the desired state of CronScaler
type CronScalerSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of CronScaler. Edit cronscaler_types.go to remove/update
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
	Replicas int32 `json:"replicas"`
	// +optional
	DefaultReplicas int32                   `json:"defaultReplicas,omitempty"`
	Deployments     []DeploymentScaleTarget `json:"deployments"`
}

type DeploymentScaleTarget struct {
	Name      string `json:"name"`
	NameSpace string `json:"namespace"`
}

type DeploymentScaleFailedStatus struct {
	Name               string      `json:"name"`
	NameSpace          string      `json:"namespace"`
	Reason             string      `json:"reason"`
	Message            string      `json:"message"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
}

// CronScalerStatus defines the observed state of CronScaler.
type CronScalerStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the CronScaler resource.
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
	// +optional
	FailedDeployments []DeploymentScaleFailedStatus `json:"failedDeployments,omitempty"`
	// +optional
	FailedDeploymentSummary string `json:"failedDeploymentSummary,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=cronscalers,singular=cronscaler
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.status"
// +kubebuilder:printcolumn:name="FailedDeployments",type="string",JSONPath=".status.failedDeploymentSummary"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// CronScaler is the Schema for the cronscalers API
type CronScaler struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of CronScaler
	// +required
	Spec CronScalerSpec `json:"spec"`

	// status defines the observed state of CronScaler
	// +optional
	Status CronScalerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// CronScalerList contains a list of CronScaler
type CronScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CronScaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CronScaler{}, &CronScalerList{})
}
