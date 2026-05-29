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

package controller

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	opcronscalev1 "github.com/example/op-cron-scale/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testMinuteTimeLayout = "2006-01-02 15:04"

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}

var _ = Describe("CronScaler Controller", func() {
	It("scales deployments to replicas during the minute-level active window", func(ctx SpecContext) {
		name := fmt.Sprintf("active-%d", time.Now().UnixNano())
		namespace := "default"
		currentMinute := time.Now().Truncate(time.Minute)
		one := int32(1)
		k8sClient, scheme := newFakeClient()

		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &one,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())

		CronScaler := &opcronscalev1.CronScaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: opcronscalev1.CronScalerSpec{
				StartTime:       currentMinute.Add(-time.Minute).Format(testMinuteTimeLayout),
				EndTime:         currentMinute.Add(time.Minute).Format(testMinuteTimeLayout),
				Replicas:        3,
				DefaultReplicas: 1,
				Deployments: []opcronscalev1.DeploymentScaleTarget{{
					Name:      name,
					NameSpace: namespace,
				}},
			},
		}
		Expect(k8sClient.Create(ctx, CronScaler)).To(Succeed())

		reconciler := &CronScalerReconciler{Client: k8sClient, Scheme: scheme}
		result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Minute))

		updated := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, updated)).To(Succeed())
		Expect(updated.Spec.Replicas).NotTo(BeNil())
		Expect(*updated.Spec.Replicas).To(Equal(int32(3)))
	})

	It("scales deployments to defaultReplicas outside the active window", func(ctx SpecContext) {
		name := fmt.Sprintf("inactive-%d", time.Now().UnixNano())
		namespace := "default"
		currentMinute := time.Now().Truncate(time.Minute)
		three := int32(3)
		k8sClient, scheme := newFakeClient()

		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &three,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())

		CronScaler := &opcronscalev1.CronScaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: opcronscalev1.CronScalerSpec{
				StartTime:       currentMinute.Add(-3 * time.Minute).Format(testMinuteTimeLayout),
				EndTime:         currentMinute.Add(-time.Minute).Format(testMinuteTimeLayout),
				Replicas:        5,
				DefaultReplicas: 1,
				Deployments: []opcronscalev1.DeploymentScaleTarget{{
					Name:      name,
					NameSpace: namespace,
				}},
			},
		}
		Expect(k8sClient.Create(ctx, CronScaler)).To(Succeed())

		reconciler := &CronScalerReconciler{Client: k8sClient, Scheme: scheme}
		result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Minute))

		updated := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, updated)).To(Succeed())
		Expect(updated.Spec.Replicas).NotTo(BeNil())
		Expect(*updated.Spec.Replicas).To(Equal(int32(1)))
	})
})

func newFakeClient() (client.Client, *runtime.Scheme) {
	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(opcronscalev1.AddToScheme(scheme)).To(Succeed())

	return fake.NewClientBuilder().WithScheme(scheme).Build(), scheme
}
