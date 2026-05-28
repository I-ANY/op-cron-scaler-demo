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
	"context"
	"time"

	opcronscalev1 "github.com/example/op-cron-scale/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const minuteTimeLayout = "2006-01-02 15:04"

// CronScaleDemoReconciler reconciles a CronScaleDemo object
type CronScaleDemoReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=op-cron-scale.op-cron-scale.example.com,resources=cronscaledemoes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=op-cron-scale.op-cron-scale.example.com,resources=cronscaledemoes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=op-cron-scale.op-cron-scale.example.com,resources=cronscaledemoes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronScaleDemo object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *CronScaleDemoReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("namespace", req.Namespace, "name", req.Name)
	logger.Info("Reconciling CronScaleDemo")

	scaler := &opcronscalev1.CronScaleDemo{}
	if err := r.Get(ctx, req.NamespacedName, scaler); err != nil {
		logger.Error(err, "unable to fetch CronScale")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	startTime, err := time.ParseInLocation(minuteTimeLayout, scaler.Spec.StartTime, time.Local)
	if err != nil {
		logger.Error(err, "invalid startTime", "startTime", scaler.Spec.StartTime)
		return ctrl.Result{}, err
	}

	endTime, err := time.ParseInLocation(minuteTimeLayout, scaler.Spec.EndTime, time.Local)
	if err != nil {
		logger.Error(err, "invalid endTime", "endTime", scaler.Spec.EndTime)
		return ctrl.Result{}, err
	}

	now := time.Now().Truncate(time.Minute)
	targetReplicas := scaler.Spec.DefaultReplicas
	if !now.Before(startTime) && !now.After(endTime) {
		targetReplicas = scaler.Spec.Replicas
	}

	for _, deployment := range scaler.Spec.Deployments {
		if err := r.scaleDeployment(ctx, deployment, targetReplicas); err != nil {
			logger.Error(err, "unable to scale Deployment", "deploymentName", deployment.Name, "namespace", deployment.NameSpace)
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func (r *CronScaleDemoReconciler) scaleDeployment(ctx context.Context, target opcronscalev1.DeploymentScaleTarget, replicas int32) error {
	logger := log.FromContext(ctx).WithValues("deploymentName", target.Name, "namespace", target.NameSpace)
	deploy := &appsv1.Deployment{}

	if err := r.Get(ctx, types.NamespacedName{Namespace: target.NameSpace, Name: target.Name}, deploy); err != nil {
		return client.IgnoreNotFound(err)
	}

	if deploy.Spec.Replicas != nil && *deploy.Spec.Replicas == replicas {
		return nil
	}

	logger.Info("updating Deployment replicas", "replicas", replicas)
	deploy.Spec.Replicas = &replicas
	return r.Update(ctx, deploy)
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronScaleDemoReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&opcronscalev1.CronScaleDemo{}).
		Named("cronscaledemo").
		Complete(r)
}
