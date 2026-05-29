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

var (
	logger = log.Log.WithName("controller_cronscaledemo")
)

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
	logger.Info("Reconciling CronScaleDemo")
	logger = logger.WithValues("namespace", req.Namespace, "name", req.Name)

	currentTimeStr := time.Now().Format(opcronscalev1.MinuteTimeLayout)
	currentTime, _ := time.Parse(opcronscalev1.MinuteTimeLayout, currentTimeStr)
	// 获取scaler
	scaler := &opcronscalev1.CronScaleDemo{}
	if err := r.Get(ctx, req.NamespacedName, scaler); err != nil {
		logger.Error(err, "unable to fetch CronScale")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// 获取配置的时间段信息
	startTime, err := time.Parse(opcronscalev1.MinuteTimeLayout, scaler.Spec.StartTime)
	if err != nil {
		logger.Error(err, "invalid startTime", "startTime", scaler.Spec.StartTime)
		return ctrl.Result{}, err
	}
	endTime, err := time.Parse(opcronscalev1.MinuteTimeLayout, scaler.Spec.EndTime)
	if err != nil {
		logger.Error(err, "invalid endTime", "endTime", scaler.Spec.EndTime)
		return ctrl.Result{}, err
	}

	// 判断当前时间是否在时间段内
	targetReplicas := scaler.Spec.DefaultReplicas
	if !currentTime.Before(startTime) && !currentTime.After(endTime) {
		targetReplicas = scaler.Spec.Replicas
	}
	// 更新deployment副本数
	for _, deployment := range scaler.Spec.Deployments {
		if err := r.scaleDeployment(ctx, *scaler, deployment, targetReplicas); err != nil {
			logger.Error(err, "unable to scale Deployment", "deploymentName", deployment.Name, "namespace", deployment.NameSpace)
			return ctrl.Result{}, err
		}
	}

	// 一分钟轮询进行更新
	//不传 RequeueAfter：不会每分钟检查，那即使时间到了，如果资源没发生变更，也不会自动触发
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// 更新deployment副本数方法
func (r *CronScaleDemoReconciler) scaleDeployment(ctx context.Context, scaler opcronscalev1.CronScaleDemo, target opcronscalev1.DeploymentScaleTarget, replicas int32) error {
	logger := log.FromContext(ctx).WithValues("deploymentName", target.Name, "namespace", target.NameSpace)
	deploy := &appsv1.Deployment{}

	// 获取deployment对象
	if err := r.Get(ctx, types.NamespacedName{Namespace: target.NameSpace, Name: target.Name}, deploy); err != nil {
		return client.IgnoreNotFound(err)
	}
	// 如果副本数一样就直接返回
	if deploy.Spec.Replicas != nil && *deploy.Spec.Replicas == replicas {
		return nil
	}

	// 更新deployment副本数
	logger.Info("updating Deployment replicas", "replicas", replicas)
	deploy.Spec.Replicas = &replicas
	if err := r.Update(ctx, deploy); err != nil {
		// 如果更新失败就把scaler 状态更新为 failed
		scaler.Status.Status = opcronscalev1.FAILED
		if err := r.Status().Update(ctx, &scaler); err != nil {
			return err
		}
	}
	//更新成功就把scaler 状态更新为 failed
	scaler.Status.Status = opcronscalev1.SUCCESS
	if err := r.Status().Update(ctx, &scaler); err != nil {
		return err
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronScaleDemoReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&opcronscalev1.CronScaleDemo{}).
		Named("cronscaledemo").
		Complete(r)
}
