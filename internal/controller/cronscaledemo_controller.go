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

	cronscalerv1 "github.com/example/op-cron-scale/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const FinalizerName = "finalizer.op-cron-scaler.example.com"

var (
	logger                 = log.Log.WithValues("Cron Scaler")
	originalDeploymentInfo = make(map[string]cronscalerv1.DeploymentInfo)
)

// CronScalerReconciler reconciles a CronScaler object
type CronScalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=op-cron-scale.op-cron-scale.example.com,resources=cronscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=op-cron-scale.op-cron-scale.example.com,resources=cronscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=op-cron-scale.op-cron-scale.example.com,resources=cronscalers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronScaler object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *CronScalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	//logger := log.FromContext(ctx).WithValues("namespace", req.Namespace, "name", req.Name)
	logger = logger.WithValues("namespace", req.Namespace, "name", req.Name)
	logger.Info("Reconciling CronScaler")

	currentTimeStr := time.Now().Format(cronscalerv1.MinuteTimeLayout)
	currentTime, _ := time.Parse(cronscalerv1.MinuteTimeLayout, currentTimeStr)
	// 获取scaler
	scaler := &cronscalerv1.CronScaler{}
	if err := r.Get(ctx, req.NamespacedName, scaler); err != nil {
		logger.Error(err, "unable to fetch Cron Scaler")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// 首先先保存初始的deploy 的副本数等信息，保存到 cron scaler 的 Annotations中
	// 先判断scaler 的状态，如果是刚开始首次启动，还没有进行任何操作

	if scaler.ObjectMeta.DeletionTimestamp.IsZero() {
		/*
			Finalizer 是 Kubernetes 的垃圾回收机制之一。当用户删除一个资源时，没有 Finalizer则资源立即被删除；
			有Finalizer：Kubernetes 不会立即删除资源，而是设置 metadata.deletionTimestamp，
			控制器检测到后执行清理逻辑（删除外部资源、恢复原始状态等），清理完成后，
			控制器从 finalizers 列表中移除该 finalizer最后 Kubernetes 才真正删除资源
		*/
		if !controllerutil.ContainsFinalizer(scaler, FinalizerName) {
			controllerutil.AddFinalizer(scaler, FinalizerName)
			logger.Info("add finalizer", "finalizer", FinalizerName)
			if err := r.Update(ctx, scaler); err != nil {
				logger.Error(err, "unable to update CronScaler")
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}
		// 判断scaler 的状态，刚开始状态，置为 running
		if scaler.Status.Status == "" {
			scaler.Status.Status = cronscalerv1.RUNNING
			if err := r.Status().Update(ctx, scaler); err != nil {
				logger.Error(err, "unable to update CronScaler status")
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
			// 保存deployment 的信息
			if err := saveDeploymentInfoIntoAnnotation(ctx, *scaler, r); err != nil {
				logger.Error(err, "unable to save original deployment info into annotation")
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}

		// 开始进行 scale
		// 获取配置的时间段信息
		startTime, err := time.Parse(cronscalerv1.MinuteTimeLayout, scaler.Spec.StartTime)
		if err != nil {
			logger.Error(err, "invalid startTime", "startTime", scaler.Spec.StartTime)
			return ctrl.Result{}, err
		}
		endTime, err := time.Parse(cronscalerv1.MinuteTimeLayout, scaler.Spec.EndTime)
		if err != nil {
			logger.Error(err, "invalid endTime", "endTime", scaler.Spec.EndTime)
			return ctrl.Result{}, err
		}
		logger.Info("current time", "currentTime", currentTime.Format(cronscalerv1.MinuteTimeLayout), "startTime", startTime.Format(cronscalerv1.MinuteTimeLayout), "endTime", endTime.Format(cronscalerv1.MinuteTimeLayout))
		// 判断当前时间是否在时间段内
		targetReplicas := scaler.Spec.DefaultReplicas
		if currentTime.After(startTime) && currentTime.Before(endTime) {
			logger.Info("start to scale deployment", "targetReplicas", scaler.Spec.Replicas)
			targetReplicas = scaler.Spec.Replicas
			// 更新deployment副本数
			for _, deployment := range scaler.Spec.Deployments {
				if err := r.scaleDeployment(ctx, *scaler, deployment, targetReplicas); err != nil {
					logger.Error(err, "unable to scale Deployment", "deploymentName", deployment.Name, "namespace", deployment.NameSpace)
					return ctrl.Result{}, err
				}
			}
		}
	} else {
		// cron scaler 被删除了
		// 如果已经更改了 deployment 的副本数，那么需要把副本数还原
		logger.Info("start to CronScaler delete")
		if scaler.Status.Status == cronscalerv1.SUCCESS {
			if err := restoreDeploymentReplicas(ctx, r, *scaler); err != nil {
				logger.Error(err, "unable to restore deployment replicas")
				return ctrl.Result{}, err
			}
		}
		logger.Info("remove finalizer")
		controllerutil.RemoveFinalizer(scaler, FinalizerName)
		if err := r.Update(ctx, scaler); err != nil {
			logger.Error(err, "unable to update CronScaler")
			return ctrl.Result{}, err
		}
		logger.Info("finalizer deleted")
		logger.Info("CronScaler deleted")
	}
	// 一分钟轮询进行更新
	//不传 RequeueAfter：不会每分钟检查，那即使时间到了，如果资源没发生变更，也不会自动触发
	return ctrl.Result{RequeueAfter: time.Second * 5}, nil
}

// 保存目标 deployment的信息
func saveDeploymentInfoIntoAnnotation(ctx context.Context, scaler cronscalerv1.CronScaler, r *CronScalerReconciler) (err error) {
	logger.Info("Start to save original deployment info into cron scaler annotation")
	deployment := appsv1.Deployment{}
	for _, depItem := range scaler.Spec.Deployments {
		if err := r.Get(ctx, types.NamespacedName{
			Name:      depItem.Name,
			Namespace: depItem.NameSpace,
		}, &deployment); err != nil {
			logger.Error(err, "unable to get Deployment", "deploymentName", depItem.Name, "namespace", depItem.NameSpace)
			continue
		}
		originalDeploy := cronscalerv1.DeploymentInfo{
			NameSpace: deployment.Name,
			Replicas:  *deployment.Spec.Replicas,
		}
		jsonData, err := json.Marshal(originalDeploy)
		if err != nil {
			return err
		}
		scaler.Annotations[depItem.Name] = string(jsonData)
	}
	return r.Update(ctx, &scaler)
}

// 更新deployment副本数方法
func (r *CronScalerReconciler) scaleDeployment(ctx context.Context, scaler cronscalerv1.CronScaler, target cronscalerv1.DeploymentScaleTarget, replicas int32) error {
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
		scaler.Status.Status = cronscalerv1.FAILED
		if err := r.Status().Update(ctx, &scaler); err != nil {
			return err
		}
	}
	//更新成功就把scaler 状态更新为 failed
	scaler.Status.Status = cronscalerv1.SUCCESS
	if err := r.Status().Update(ctx, &scaler); err != nil {
		return err
	}
	return nil
}

// 重置 deployment的信息
func restoreDeploymentReplicas(ctx context.Context, r *CronScalerReconciler, scaler cronscalerv1.CronScaler) error {
	logger.Info("Start to restore deployment replicas")
	for _, depItem := range scaler.Spec.Deployments {
		// 从 scaler 的annotation 中获取原来的副本数
		jsonData, ok := scaler.Annotations[depItem.Name]
		if !ok {
			logger.Info("no original deployment info found in annotation", "deploymentName", depItem.Name)
			continue
		}
		var originalDeploy cronscalerv1.DeploymentInfo
		if err := json.Unmarshal([]byte(jsonData), &originalDeploy); err != nil {
			logger.Error(err, "unable to unmarshal original deployment info from annotation", "deploymentName", depItem.Name)
			continue
		}
		deployment := appsv1.Deployment{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      depItem.Name,
			Namespace: originalDeploy.NameSpace,
		}, &deployment); err != nil {
			logger.Error(err, "unable to get Deployment", "deploymentName", originalDeploy.Name, "namespace", originalDeploy.NameSpace)
			continue
		}
		if *deployment.Spec.Replicas != originalDeploy.Replicas {
			deployment.Spec.Replicas = &originalDeploy.Replicas
			if err := r.Update(ctx, &deployment); err != nil {
				logger.Error(err, "unable to update Deployment", "deploymentName", originalDeploy.Name, "namespace", originalDeploy.NameSpace)
				continue
			}
		}
	}
	// 修改状态
	scaler.Status.Status = cronscalerv1.RESTORED
	return r.Update(ctx, &scaler)
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cronscalerv1.CronScaler{}).
		Named("CronScaler").
		Complete(r)
}
