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
	"strings"
	"time"

	cronscalerv1 "github.com/example/op-cron-scale/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const FinalizerName = "finalizer.op-cron-scaler.example.com"

const (
	deploymentInfoReasonGetFailed      = "GetDeploymentFailed"
	deploymentInfoReasonNotFound       = "DeploymentNotFound"
	deploymentInfoReasonReplicasNotSet = "DeploymentReplicasNotSet"
)

type deploymentInfoAnnotation struct {
	Replicas  *int32 `json:"replicas,omitempty"`
	NameSpace string `json:"namespace"`
	Name      string `json:"name"`
	Reason    string `json:"reason,omitempty"`
	Message   string `json:"message,omitempty"`
}

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
	reconcileLogger := log.FromContext(ctx)
	reconcileLogger.Info("Reconciling CronScaler")

	currentTimeStr := time.Now().Format(cronscalerv1.MinuteTimeLayout)
	currentTime, _ := time.Parse(cronscalerv1.MinuteTimeLayout, currentTimeStr)
	// 获取scaler
	scaler := &cronscalerv1.CronScaler{}
	if err := r.Get(ctx, req.NamespacedName, scaler); err != nil {
		// 资源可能已经被删除，删除事件无需继续处理。
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		reconcileLogger.Error(err, "Unable to fetch CronScaler")
		return ctrl.Result{}, err
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
			reconcileLogger.Info("Adding finalizer", "finalizer", FinalizerName)
			if err := r.Update(ctx, scaler); err != nil {
				// 并发删除时对象已不存在，不需要重试更新 finalizer。
				if apierrors.IsNotFound(err) {
					return ctrl.Result{}, nil
				}
				reconcileLogger.Error(err, "Unable to update CronScaler")
				return ctrl.Result{}, err
			}
			// metadata 更新后重新入队，避免继续使用旧 resourceVersion 更新 status
			return ctrl.Result{Requeue: true}, nil
		}
		if scaler.Status.Status == "" {
			scaler.Status.Status = cronscalerv1.RUNNING
			if err := r.Status().Update(ctx, scaler); err != nil {
				// 并发删除时对象已不存在，不需要再写入状态。
				if apierrors.IsNotFound(err) {
					return ctrl.Result{}, nil
				}
				reconcileLogger.Error(err, "Unable to update CronScaler status")
				return ctrl.Result{}, err
			}
			// status 更新后重新入队，避免继续使用旧 resourceVersion 更新 annotations
			return ctrl.Result{RequeueAfter: time.Millisecond}, nil
		}
		// 只在初始化阶段补齐缺失记录，避免把已经扩缩容后的副本数误存成原始值。
		if scaler.Status.Status == cronscalerv1.RUNNING && needsDeploymentInfoRefresh(*scaler, true) {
			saved, err := saveDeploymentInfoIntoAnnotation(ctx, *scaler, r, true)
			if err != nil {
				// 并发删除时对象已不存在，不需要再保存初始化信息。
				if apierrors.IsNotFound(err) {
					return ctrl.Result{}, nil
				}
				reconcileLogger.Error(err, "Unable to save original Deployment info into annotation")
				return ctrl.Result{}, err
			}
			if saved {
				// annotations 更新后重新入队，避免继续使用旧 resourceVersion 更新 status。
				return ctrl.Result{RequeueAfter: time.Millisecond}, nil
			}
		}
		if scaler.Status.Status != cronscalerv1.RUNNING && needsDeploymentInfoRefresh(*scaler, false) {
			// 已经记录失败原因的 Deployment 后续可能被创建出来，需要重试并用真实副本数替换失败记录。
			saved, err := saveDeploymentInfoIntoAnnotation(ctx, *scaler, r, false)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return ctrl.Result{}, nil
				}
				reconcileLogger.Error(err, "Unable to refresh original Deployment info annotation")
				return ctrl.Result{}, err
			}
			if saved {
				return ctrl.Result{RequeueAfter: time.Millisecond}, nil
			}
		}

		// 开始进行 scale
		// 获取配置的时间段信息
		startTime, err := time.Parse(cronscalerv1.MinuteTimeLayout, scaler.Spec.StartTime)
		if err != nil {
			reconcileLogger.Error(err, "Invalid startTime", "startTime", scaler.Spec.StartTime)
			return ctrl.Result{}, err
		}
		endTime, err := time.Parse(cronscalerv1.MinuteTimeLayout, scaler.Spec.EndTime)
		if err != nil {
			reconcileLogger.Error(err, "Invalid endTime", "endTime", scaler.Spec.EndTime)
			return ctrl.Result{}, err
		}
		reconcileLogger.Info("Checked scale window", "currentTime", currentTime.Format(cronscalerv1.MinuteTimeLayout), "startTime", startTime.Format(cronscalerv1.MinuteTimeLayout), "endTime", endTime.Format(cronscalerv1.MinuteTimeLayout))
		// 判断当前时间是否在时间段内
		if isWithinScaleWindow(currentTime, startTime, endTime) {
			reconcileLogger.Info("Scaling Deployments", "targetReplicas", scaler.Spec.Replicas)
			// 更新deployment副本数，并记录本轮扩缩容失败的 deployment
			failedDeployments := make([]cronscalerv1.DeploymentScaleFailedStatus, 0)
			for _, deployment := range scaler.Spec.Deployments {
				if failedDeployment := deploymentInfoUnavailableStatus(*scaler, deployment); failedDeployment != nil {
					failedDeployments = append(failedDeployments, *failedDeployment)
					continue
				}
				if err := r.scaleDeployment(ctx, deployment, scaler.Spec.Replicas); err != nil {
					reconcileLogger.Error(err, "Unable to scale Deployment", "deploymentName", deployment.Name, "deploymentNamespace", deployment.NameSpace)
					failedDeployments = append(failedDeployments, cronscalerv1.DeploymentScaleFailedStatus{
						Name:               deployment.Name,
						NameSpace:          deployment.NameSpace,
						Reason:             "UpdateDeploymentFailed",
						Message:            err.Error(),
						LastTransitionTime: metav1.Now(),
					})
				}
			}
			if err := updateScaleStatus(ctx, r, *scaler, failedDeployments); err != nil {
				reconcileLogger.Error(err, "Unable to update CronScaler scale status")
				return ctrl.Result{}, err
			}
		} else if shouldRestoreDeployments(scaler.Status.Status) {
			// 不在时间范围内时，如果已经扩缩容过，需要恢复 deployment 原始副本数
			if err := restoreDeploymentReplicas(ctx, r, *scaler); err != nil {
				reconcileLogger.Error(err, "Unable to restore Deployment replicas")
				return ctrl.Result{}, err
			}
		}
	} else {
		// cron scaler 被删除了
		// 如果已经更改了 deployment 的副本数，那么需要把副本数还原
		reconcileLogger.Info("Deleting CronScaler")
		if shouldRestoreDeployments(scaler.Status.Status) {
			if err := restoreDeploymentReplicas(ctx, r, *scaler); err != nil {
				reconcileLogger.Error(err, "Unable to restore Deployment replicas")
				return ctrl.Result{}, err
			}
		}
		reconcileLogger.Info("Removing finalizer", "finalizer", FinalizerName)
		controllerutil.RemoveFinalizer(scaler, FinalizerName)
		if err := r.Update(ctx, scaler); err != nil {
			// 并发删除时对象已不存在，finalizer 清理流程可以结束。
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			reconcileLogger.Error(err, "Unable to update CronScaler")
			return ctrl.Result{}, err
		}
		reconcileLogger.Info("Removed finalizer", "finalizer", FinalizerName)
		reconcileLogger.Info("Deleted CronScaler")
	}
	// 一分钟轮询进行更新
	//不传 RequeueAfter：不会每分钟检查，那即使时间到了，如果资源没发生变更，也不会自动触发
	return ctrl.Result{RequeueAfter: time.Second * 5}, nil
}

// 判断当前时间是否在扩缩容时间范围内，支持跨天时间段
func isWithinScaleWindow(currentTime, startTime, endTime time.Time) bool {
	if startTime.Equal(endTime) {
		return true
	}
	if startTime.Before(endTime) {
		return !currentTime.Before(startTime) && currentTime.Before(endTime)
	}
	return !currentTime.Before(startTime) || currentTime.Before(endTime)
}

func needsDeploymentInfoRefresh(scaler cronscalerv1.CronScaler, includeMissing bool) bool {
	// 不能只判断 annotations 是否为空，kubectl apply 会写入自己的 last-applied annotation。
	for _, deployment := range scaler.Spec.Deployments {
		info, ok := readDeploymentInfoAnnotation(scaler, deployment)
		if !ok {
			if includeMissing {
				return true
			}
			continue
		}
		if info.Replicas == nil {
			return true
		}
	}
	return false
}

func shouldRestoreDeployments(status string) bool {
	// Failed 也可能代表部分 Deployment 已经扩缩容成功，需要在窗口结束或删除时恢复。
	return status == cronscalerv1.SUCCESS || status == cronscalerv1.FAILED
}

// 保存目标 Deployment 的原始副本数；返回值表示本轮是否写入了新的 annotation。
func saveDeploymentInfoIntoAnnotation(ctx context.Context, scaler cronscalerv1.CronScaler, r *CronScalerReconciler, includeMissing bool) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("Saving original Deployment info into annotation")
	// 如果用户创建 CronScaler 时没有配置 annotations，这里需要先初始化再写入
	if scaler.Annotations == nil {
		scaler.Annotations = make(map[string]string)
	}
	saved := false
	deployment := appsv1.Deployment{}
	for _, depItem := range scaler.Spec.Deployments {
		currentInfo, ok := readDeploymentInfoAnnotation(scaler, depItem)
		if !ok && !includeMissing {
			continue
		}
		// 已成功保存过的 Deployment 不再覆盖，避免后续 reconcile 改写原始副本数。
		if ok && currentInfo.Replicas != nil {
			continue
		}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      depItem.Name,
			Namespace: depItem.NameSpace,
		}, &deployment); err != nil {
			if apierrors.IsNotFound(err) {
				// 缺失的 Deployment 不阻塞其它 Deployment 初始化，但要记录原因供后续排查。
				wrote, err := saveDeploymentInfoAnnotationFailure(scaler.Annotations, depItem, deploymentInfoReasonNotFound, err.Error())
				if err != nil {
					return false, err
				}
				saved = saved || wrote
				logger.Info("Deployment not found while saving original info", "deploymentName", depItem.Name, "deploymentNamespace", depItem.NameSpace)
				continue
			}
			// 非 NotFound 的读取失败也写入 annotation，避免下一轮只能看到反复报错。
			wrote, err := saveDeploymentInfoAnnotationFailure(scaler.Annotations, depItem, deploymentInfoReasonGetFailed, err.Error())
			if err != nil {
				return false, err
			}
			saved = saved || wrote
			logger.Error(err, "Unable to get Deployment", "deploymentName", depItem.Name, "deploymentNamespace", depItem.NameSpace)
			continue
		}
		if deployment.Spec.Replicas == nil {
			// 没有显式副本数时无法可靠恢复，所以记录失败原因而不是写入错误的默认值。
			wrote, err := saveDeploymentInfoAnnotationFailure(scaler.Annotations, depItem, deploymentInfoReasonReplicasNotSet, "Deployment spec.replicas is nil")
			if err != nil {
				return false, err
			}
			saved = saved || wrote
			logger.Info("Deployment replicas not set while saving original info", "deploymentName", depItem.Name, "deploymentNamespace", depItem.NameSpace)
			continue
		}
		originalDeploy := deploymentInfoAnnotation{
			Replicas:  deployment.Spec.Replicas,
			NameSpace: deployment.Namespace,
			Name:      deployment.Name,
		}
		jsonData, err := json.Marshal(originalDeploy)
		if err != nil {
			return false, err
		}
		newAnnotation := string(jsonData)
		if scaler.Annotations[depItem.Name] != newAnnotation {
			scaler.Annotations[depItem.Name] = newAnnotation
			saved = true
		}
	}
	if !saved {
		// 没有新增 annotation 时不执行 Update，避免无意义写入触发循环。
		return false, nil
	}
	return true, r.Update(ctx, &scaler)
}

func saveDeploymentInfoAnnotationFailure(annotations map[string]string, deployment cronscalerv1.DeploymentScaleTarget, reason, message string) (bool, error) {
	// 失败记录和成功记录放在同一个 annotation key，后续 reconcile 可以直接知道该 Deployment 为什么没有原始副本数。
	failedInfo := deploymentInfoAnnotation{
		NameSpace: deployment.NameSpace,
		Name:      deployment.Name,
		Reason:    reason,
		Message:   message,
	}
	jsonData, err := json.Marshal(failedInfo)
	if err != nil {
		return false, err
	}
	newAnnotation := string(jsonData)
	if annotations[deployment.Name] == newAnnotation {
		return false, nil
	}
	annotations[deployment.Name] = newAnnotation
	return true, nil
}

func readDeploymentInfoAnnotation(scaler cronscalerv1.CronScaler, deployment cronscalerv1.DeploymentScaleTarget) (deploymentInfoAnnotation, bool) {
	jsonData, ok := scaler.Annotations[deployment.Name]
	if !ok {
		return deploymentInfoAnnotation{}, false
	}
	var info deploymentInfoAnnotation
	if err := json.Unmarshal([]byte(jsonData), &info); err != nil {
		// annotation 被人工改坏时当作需要刷新，下一轮会重新写入可读的原因或副本数。
		return deploymentInfoAnnotation{
			Name:      deployment.Name,
			NameSpace: deployment.NameSpace,
			Reason:    deploymentInfoReasonGetFailed,
			Message:   err.Error(),
		}, true
	}
	return info, true
}

func deploymentInfoUnavailableStatus(scaler cronscalerv1.CronScaler, deployment cronscalerv1.DeploymentScaleTarget) *cronscalerv1.DeploymentScaleFailedStatus {
	info, ok := readDeploymentInfoAnnotation(scaler, deployment)
	if ok && info.Replicas != nil {
		return nil
	}
	reason := "OriginalDeploymentInfoUnavailable"
	message := "Original Deployment replicas were not saved"
	if ok && info.Reason != "" {
		reason = info.Reason
		message = info.Message
	}
	// 没有原始副本数的 Deployment 不参与扩缩容，避免后续无法可靠恢复。
	return &cronscalerv1.DeploymentScaleFailedStatus{
		Name:               deployment.Name,
		NameSpace:          deployment.NameSpace,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
}

// 更新deployment副本数方法
func (r *CronScalerReconciler) scaleDeployment(ctx context.Context, target cronscalerv1.DeploymentScaleTarget, replicas int32) error {
	logger := log.FromContext(ctx)
	deploy := &appsv1.Deployment{}

	// 获取deployment对象
	if err := r.Get(ctx, types.NamespacedName{Namespace: target.NameSpace, Name: target.Name}, deploy); err != nil {
		// 扩缩容阶段不能忽略 NotFound，要让 status 记录具体失败的 Deployment。
		return err
	}
	// 如果副本数一样就直接返回
	if deploy.Spec.Replicas != nil && *deploy.Spec.Replicas == replicas {
		return nil
	}

	// 更新deployment副本数
	logger.Info("Updating Deployment replicas", "deploymentName", target.Name, "deploymentNamespace", target.NameSpace, "replicas", replicas)
	deploy.Spec.Replicas = &replicas
	if err := r.Update(ctx, deploy); err != nil {
		return err
	}
	return nil
}

// 根据本轮扩缩容失败结果更新 CronScaler 状态
func updateScaleStatus(ctx context.Context, r *CronScalerReconciler, scaler cronscalerv1.CronScaler, failedDeployments []cronscalerv1.DeploymentScaleFailedStatus) error {
	if len(failedDeployments) == 0 {
		scaler.Status.Status = cronscalerv1.SUCCESS
		scaler.Status.FailedDeployments = nil
		scaler.Status.FailedDeploymentSummary = ""
		// 扩缩容完成后对象可能已被删除，NotFound 不应触发 Reconciler error。
		return client.IgnoreNotFound(r.Status().Update(ctx, &scaler))
	}

	scaler.Status.Status = cronscalerv1.FAILED
	scaler.Status.FailedDeployments = failedDeployments
	scaler.Status.FailedDeploymentSummary = buildFailedDeploymentSummary(failedDeployments)
	// 记录失败状态时对象可能已被删除，NotFound 不应触发 Reconciler error。
	return client.IgnoreNotFound(r.Status().Update(ctx, &scaler))
}

// 构建用于 kubectl get 展示的失败 deployment 摘要
func buildFailedDeploymentSummary(failedDeployments []cronscalerv1.DeploymentScaleFailedStatus) string {
	summaries := make([]string, 0, len(failedDeployments))
	for _, deployment := range failedDeployments {
		summaries = append(summaries, deployment.NameSpace+"/"+deployment.Name)
	}
	return strings.Join(summaries, ",")
}

// 重置 deployment的信息
func restoreDeploymentReplicas(ctx context.Context, r *CronScalerReconciler, scaler cronscalerv1.CronScaler) error {
	logger := log.FromContext(ctx)
	logger.Info("Restoring Deployment replicas")
	for _, depItem := range scaler.Spec.Deployments {
		// 从 scaler 的annotation 中获取原来的副本数
		jsonData, ok := scaler.Annotations[depItem.Name]
		if !ok {
			logger.Info("Original Deployment info not found in annotation", "deploymentName", depItem.Name, "deploymentNamespace", depItem.NameSpace)
			continue
		}
		var originalDeploy deploymentInfoAnnotation
		if err := json.Unmarshal([]byte(jsonData), &originalDeploy); err != nil {
			logger.Error(err, "Unable to unmarshal original Deployment info from annotation", "deploymentName", depItem.Name, "deploymentNamespace", depItem.NameSpace)
			continue
		}
		if originalDeploy.Replicas == nil {
			// 只有失败原因、没有 replicas 的记录不能用于恢复，避免把缺失信息误恢复成 0。
			logger.Info("Original Deployment replicas unavailable", "deploymentName", originalDeploy.Name, "deploymentNamespace", originalDeploy.NameSpace, "reason", originalDeploy.Reason, "message", originalDeploy.Message)
			continue
		}
		deployment := appsv1.Deployment{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      depItem.Name,
			Namespace: originalDeploy.NameSpace,
		}, &deployment); err != nil {
			logger.Error(err, "Unable to get Deployment", "deploymentName", originalDeploy.Name, "deploymentNamespace", originalDeploy.NameSpace)
			continue
		}
		if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != *originalDeploy.Replicas {
			// replicas 为空时也显式写回原始值，保证恢复后的 spec 可预期。
			deployment.Spec.Replicas = originalDeploy.Replicas
			if err := r.Update(ctx, &deployment); err != nil {
				logger.Error(err, "Unable to update Deployment", "deploymentName", originalDeploy.Name, "deploymentNamespace", originalDeploy.NameSpace)
				continue
			}
		}
	}
	// 修改状态
	scaler.Status.Status = cronscalerv1.RESTORED
	// 恢复副本数后只更新 status；对象并发删除时忽略 NotFound。
	return client.IgnoreNotFound(r.Status().Update(ctx, &scaler))
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cronscalerv1.CronScaler{}).
		Named("CronScaler").
		Complete(r)
}
