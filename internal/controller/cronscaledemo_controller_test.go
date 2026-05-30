package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	cronscalerv1 "github.com/example/op-cron-scale/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestSaveDeploymentInfoIntoAnnotationInitializesAnnotationsAndStoresNamespace(t *testing.T) {
	scheme := newTestScheme(t)
	deploymentReplicas := int32(3)
	scaler := cronscalerv1.CronScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec: cronscalerv1.CronScalerSpec{
			Deployments: []cronscalerv1.DeploymentScaleTarget{{Name: "web", NameSpace: "apps"}},
		},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Replicas: &deploymentReplicas},
	}
	reconciler := &CronScalerReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&cronscalerv1.CronScaler{}).WithObjects(&scaler, &deployment).Build(),
		Scheme: scheme,
	}

	if err := saveDeploymentInfoIntoAnnotation(context.Background(), scaler, reconciler); err != nil {
		t.Fatalf("saveDeploymentInfoIntoAnnotation returned error: %v", err)
	}

	var updated cronscalerv1.CronScaler
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "sample", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get updated CronScaler returned error: %v", err)
	}
	annotation := updated.Annotations["web"]
	if annotation != `{"replicas":3,"namespace":"apps","name":"web"}` {
		t.Fatalf("annotation = %s, want deployment namespace and name", annotation)
	}
}

func TestIsWithinScaleWindowIncludesStartAndSupportsCrossMidnight(t *testing.T) {
	parseTime := func(value string) time.Time {
		parsed, err := time.Parse(cronscalerv1.MinuteTimeLayout, value)
		if err != nil {
			t.Fatalf("parse time %s: %v", value, err)
		}
		return parsed
	}

	tests := []struct {
		name  string
		now   string
		start string
		end   string
		want  bool
	}{
		{name: "includes start minute", now: "09:00", start: "09:00", end: "18:00", want: true},
		{name: "excludes end minute", now: "18:00", start: "09:00", end: "18:00", want: false},
		{name: "cross midnight before midnight", now: "23:30", start: "23:00", end: "02:00", want: true},
		{name: "cross midnight after midnight", now: "01:30", start: "23:00", end: "02:00", want: true},
		{name: "cross midnight outside", now: "12:00", start: "23:00", end: "02:00", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isWithinScaleWindow(parseTime(tt.now), parseTime(tt.start), parseTime(tt.end))
			if got != tt.want {
				t.Fatalf("isWithinScaleWindow(%s, %s, %s) = %v, want %v", tt.now, tt.start, tt.end, got, tt.want)
			}
		})
	}
}

func TestScaleDeploymentReturnsUpdateError(t *testing.T) {
	scheme := newTestScheme(t)
	deploymentReplicas := int32(1)
	updateErr := errors.New("update failed")
	scaler := cronscalerv1.CronScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec: cronscalerv1.CronScalerSpec{
			Deployments: []cronscalerv1.DeploymentScaleTarget{{Name: "web", NameSpace: "apps"}},
		},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Replicas: &deploymentReplicas},
	}
	reconciler := &CronScalerReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(&scaler, &deployment).WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok {
					return updateErr
				}
				return c.Update(ctx, obj, opts...)
			},
		}).Build(),
		Scheme: scheme,
	}

	err := reconciler.scaleDeployment(context.Background(), cronscalerv1.DeploymentScaleTarget{Name: "web", NameSpace: "apps"}, 5)
	if !errors.Is(err, updateErr) {
		t.Fatalf("scaleDeployment error = %v, want update error", err)
	}
}

func TestReconcileRestoresDeploymentAfterScaleWindow(t *testing.T) {
	scheme := newTestScheme(t)
	originalReplicas := int32(2)
	scaledReplicas := int32(5)
	annotation := `{"replicas":2,"namespace":"apps","name":"web"}`
	scaler := cronscalerv1.CronScaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "sample",
			Namespace:   "default",
			Annotations: map[string]string{"web": annotation},
		},
		Spec: cronscalerv1.CronScalerSpec{
			StartTime: "00:00",
			EndTime:   "00:01",
			Replicas:  scaledReplicas,
			Deployments: []cronscalerv1.DeploymentScaleTarget{
				{Name: "web", NameSpace: "apps"},
			},
		},
		Status: cronscalerv1.CronScalerStatus{Status: cronscalerv1.SUCCESS},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Replicas: &scaledReplicas},
	}
	reconciler := &CronScalerReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&cronscalerv1.CronScaler{}).WithObjects(&scaler, &deployment).Build(),
		Scheme: scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "sample", Namespace: "default"}})
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var updatedDeployment appsv1.Deployment
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "web", Namespace: "apps"}, &updatedDeployment); err != nil {
		t.Fatalf("get updated Deployment returned error: %v", err)
	}
	if updatedDeployment.Spec.Replicas == nil || *updatedDeployment.Spec.Replicas != originalReplicas {
		t.Fatalf("replicas = %v, want %d", updatedDeployment.Spec.Replicas, originalReplicas)
	}
}

func TestReconcileRecordsFailedDeploymentsAndContinuesScaling(t *testing.T) {
	scheme := newTestScheme(t)
	initialReplicas := int32(1)
	targetReplicas := int32(5)
	updateErr := errors.New("update failed")
	scaler := cronscalerv1.CronScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec: cronscalerv1.CronScalerSpec{
			StartTime: "00:00",
			EndTime:   "00:00",
			Replicas:  targetReplicas,
			Deployments: []cronscalerv1.DeploymentScaleTarget{
				{Name: "web", NameSpace: "apps"},
				{Name: "api", NameSpace: "apps"},
			},
		},
		Status: cronscalerv1.CronScalerStatus{Status: cronscalerv1.RUNNING},
	}
	webDeployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Replicas: &initialReplicas},
	}
	apiDeployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Replicas: &initialReplicas},
	}
	reconciler := &CronScalerReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&cronscalerv1.CronScaler{}).WithObjects(&scaler, &webDeployment, &apiDeployment).WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if deployment, ok := obj.(*appsv1.Deployment); ok && deployment.Name == "web" {
					return updateErr
				}
				return c.Update(ctx, obj, opts...)
			},
		}).Build(),
		Scheme: scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "sample", Namespace: "default"}})
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var updatedScaler cronscalerv1.CronScaler
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "sample", Namespace: "default"}, &updatedScaler); err != nil {
		t.Fatalf("get updated CronScaler returned error: %v", err)
	}
	if updatedScaler.Status.Status != cronscalerv1.FAILED {
		t.Fatalf("status = %s, want %s", updatedScaler.Status.Status, cronscalerv1.FAILED)
	}
	if updatedScaler.Status.FailedDeploymentSummary != "apps/web" {
		t.Fatalf("failed summary = %s, want apps/web", updatedScaler.Status.FailedDeploymentSummary)
	}
	if len(updatedScaler.Status.FailedDeployments) != 1 {
		t.Fatalf("failed deployments count = %d, want 1", len(updatedScaler.Status.FailedDeployments))
	}
	failedDeployment := updatedScaler.Status.FailedDeployments[0]
	if failedDeployment.Name != "web" || failedDeployment.NameSpace != "apps" {
		t.Fatalf("failed deployment = %s/%s, want apps/web", failedDeployment.NameSpace, failedDeployment.Name)
	}
	if failedDeployment.Reason != "UpdateDeploymentFailed" {
		t.Fatalf("reason = %s, want UpdateDeploymentFailed", failedDeployment.Reason)
	}
	if failedDeployment.Message != updateErr.Error() {
		t.Fatalf("message = %s, want %s", failedDeployment.Message, updateErr.Error())
	}
	if failedDeployment.LastTransitionTime.IsZero() {
		t.Fatalf("lastTransitionTime is zero")
	}

	var updatedAPI appsv1.Deployment
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "api", Namespace: "apps"}, &updatedAPI); err != nil {
		t.Fatalf("get updated api Deployment returned error: %v", err)
	}
	if updatedAPI.Spec.Replicas == nil || *updatedAPI.Spec.Replicas != targetReplicas {
		t.Fatalf("api replicas = %v, want %d", updatedAPI.Spec.Replicas, targetReplicas)
	}
}

func TestReconcileClearsFailedDeploymentsAfterSuccessfulScaling(t *testing.T) {
	scheme := newTestScheme(t)
	initialReplicas := int32(1)
	targetReplicas := int32(5)
	scaler := cronscalerv1.CronScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec: cronscalerv1.CronScalerSpec{
			StartTime: "00:00",
			EndTime:   "00:00",
			Replicas:  targetReplicas,
			Deployments: []cronscalerv1.DeploymentScaleTarget{
				{Name: "web", NameSpace: "apps"},
			},
		},
		Status: cronscalerv1.CronScalerStatus{
			Status:                  cronscalerv1.FAILED,
			FailedDeploymentSummary: "apps/web",
			FailedDeployments: []cronscalerv1.DeploymentScaleFailedStatus{
				{
					Name:               "web",
					NameSpace:          "apps",
					Reason:             "UpdateDeploymentFailed",
					Message:            "update failed",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Replicas: &initialReplicas},
	}
	reconciler := &CronScalerReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&cronscalerv1.CronScaler{}).WithObjects(&scaler, &deployment).Build(),
		Scheme: scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "sample", Namespace: "default"}})
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var updatedScaler cronscalerv1.CronScaler
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: "sample", Namespace: "default"}, &updatedScaler); err != nil {
		t.Fatalf("get updated CronScaler returned error: %v", err)
	}
	if updatedScaler.Status.Status != cronscalerv1.SUCCESS {
		t.Fatalf("status = %s, want %s", updatedScaler.Status.Status, cronscalerv1.SUCCESS)
	}
	if updatedScaler.Status.FailedDeploymentSummary != "" {
		t.Fatalf("failed summary = %s, want empty", updatedScaler.Status.FailedDeploymentSummary)
	}
	if len(updatedScaler.Status.FailedDeployments) != 0 {
		t.Fatalf("failed deployments count = %d, want 0", len(updatedScaler.Status.FailedDeployments))
	}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := cronscalerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add CronScaler scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Deployment scheme: %v", err)
	}
	return scheme
}
