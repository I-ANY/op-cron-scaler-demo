# Reconcile Minute Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize `CronScaleDemoReconciler.Reconcile` so it uses minute-level time windows, scales deployments through one shared path, and has the RBAC needed to update deployments.

**Architecture:** Keep the existing string API fields and parse them with a single minute-level layout. `Reconcile` will choose one target replica count based on the parsed time window, then call a helper that fetches and updates each deployment only when needed. The controller will requeue every minute because the desired scheduling precision is minute-level.

**Tech Stack:** Go, controller-runtime, Kubernetes `apps/v1.Deployment`, Ginkgo/Gomega tests, Kubebuilder/controller-gen RBAC markers.

---

### Task 1: Add Reconcile Tests for Minute-Level Scaling

**Files:**
- Modify: `internal/controller/cronscaledemo_controller_test.go`
- Exercise: `internal/controller/cronscaledemo_controller.go`

- [ ] **Step 1: Read the current controller test file**

Open `internal/controller/cronscaledemo_controller_test.go` and keep the existing package/import/style. The file already uses Ginkgo and Gomega.

- [ ] **Step 2: Add imports used by the tests**

Ensure the import block contains these packages in addition to existing imports:

```go
import (
    "context"
    "fmt"
    "time"

    opcronscalev1 "github.com/example/op-cron-scale/api/v1"
    appsv1 "k8s.io/api/apps/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/types"
    ctrl "sigs.k8s.io/controller-runtime"
)
```

- [ ] **Step 3: Add the active-window failing test**

Add this `It` block inside the existing `Describe("CronScaleDemo Controller", ...)` block:

```go
It("scales deployments to replicas during the minute-level active window", func(ctx SpecContext) {
    name := fmt.Sprintf("active-%d", time.Now().UnixNano())
    namespace := "default"
    currentMinute := time.Now().Truncate(time.Minute)
    one := int32(1)

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

    cronScaleDemo := &opcronscalev1.CronScaleDemo{
        ObjectMeta: metav1.ObjectMeta{
            Name:      name,
            Namespace: namespace,
        },
        Spec: opcronscalev1.CronScaleDemoSpec{
            StartTime:       currentMinute.Add(-time.Minute).Format(minuteTimeLayout),
            EndTime:         currentMinute.Add(time.Minute).Format(minuteTimeLayout),
            Replicas:        3,
            DefaultReplicas: 1,
            Deployments: []opcronscalev1.DeploymentScaleTarget{{
                Name:      name,
                NameSpace: namespace,
            }},
        },
    }
    Expect(k8sClient.Create(ctx, cronScaleDemo)).To(Succeed())

    reconciler := &CronScaleDemoReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
    result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
    Expect(err).NotTo(HaveOccurred())
    Expect(result.RequeueAfter).To(Equal(time.Minute))

    updated := &appsv1.Deployment{}
    Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, updated)).To(Succeed())
    Expect(updated.Spec.Replicas).NotTo(BeNil())
    Expect(*updated.Spec.Replicas).To(Equal(int32(3)))
})
```

- [ ] **Step 4: Add missing corev1 import if needed**

The test code uses `corev1.PodTemplateSpec`, `corev1.PodSpec`, and `corev1.Container`. Add this import:

```go
corev1 "k8s.io/api/core/v1"
```

- [ ] **Step 5: Run the active-window test and verify RED**

Run:

```bash
go test ./internal/controller -run 'TestControllers/CronScaleDemo_Controller/scales_deployments_to_replicas_during_the_minute-level_active_window'
```

Expected: FAIL because `minuteTimeLayout` does not exist yet, or because current `Reconcile` still returns `5 * time.Second`.

- [ ] **Step 6: Add the outside-window failing test**

Add this second `It` block inside the same `Describe` block:

```go
It("scales deployments to defaultReplicas outside the active window", func(ctx SpecContext) {
    name := fmt.Sprintf("inactive-%d", time.Now().UnixNano())
    namespace := "default"
    currentMinute := time.Now().Truncate(time.Minute)
    three := int32(3)

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

    cronScaleDemo := &opcronscalev1.CronScaleDemo{
        ObjectMeta: metav1.ObjectMeta{
            Name:      name,
            Namespace: namespace,
        },
        Spec: opcronscalev1.CronScaleDemoSpec{
            StartTime:       currentMinute.Add(-3 * time.Minute).Format(minuteTimeLayout),
            EndTime:         currentMinute.Add(-time.Minute).Format(minuteTimeLayout),
            Replicas:        5,
            DefaultReplicas: 1,
            Deployments: []opcronscalev1.DeploymentScaleTarget{{
                Name:      name,
                NameSpace: namespace,
            }},
        },
    }
    Expect(k8sClient.Create(ctx, cronScaleDemo)).To(Succeed())

    reconciler := &CronScaleDemoReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
    result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
    Expect(err).NotTo(HaveOccurred())
    Expect(result.RequeueAfter).To(Equal(time.Minute))

    updated := &appsv1.Deployment{}
    Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, updated)).To(Succeed())
    Expect(updated.Spec.Replicas).NotTo(BeNil())
    Expect(*updated.Spec.Replicas).To(Equal(int32(1)))
})
```

- [ ] **Step 7: Run the outside-window test and verify RED**

Run:

```bash
go test ./internal/controller -run 'TestControllers/CronScaleDemo_Controller/scales_deployments_to_defaultReplicas_outside_the_active_window'
```

Expected: FAIL because `minuteTimeLayout` does not exist yet, or because current `Reconcile` still returns `5 * time.Second`.

---

### Task 2: Implement Minute-Level Reconcile Logic

**Files:**
- Modify: `internal/controller/cronscaledemo_controller.go`
- Test: `internal/controller/cronscaledemo_controller_test.go`

- [ ] **Step 1: Add the minute layout constant**

In `internal/controller/cronscaledemo_controller.go`, after the imports and before `CronScaleDemoReconciler`, add:

```go
const minuteTimeLayout = "2006-01-02 15:04"
```

- [ ] **Step 2: Fix logger values and parse minute-level times**

Replace the start of `Reconcile` through the target replica decision with this code:

```go
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
```

- [ ] **Step 3: Replace duplicated deployment loops with one loop**

Immediately after the code from Step 2, add this loop and return:

```go
    for _, deployment := range scaler.Spec.Deployments {
        if err := r.scaleDeployment(ctx, deployment, targetReplicas); err != nil {
            logger.Error(err, "unable to scale Deployment", "deploymentName", deployment.Name, "namespace", deployment.NameSpace)
            return ctrl.Result{}, err
        }
    }

    return ctrl.Result{RequeueAfter: time.Minute}, nil
}
```

Remove the old `timeNow := ...`, `if startTime <= timeNow...`, both duplicated deployment loops, and the old `return ctrl.Result{RequeueAfter: 5 * time.Second}, nil`.

- [ ] **Step 4: Add the shared deployment scaling helper**

Below `Reconcile` and above `SetupWithManager`, add:

```go
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
```

- [ ] **Step 5: Update the apps import alias**

Change the deployment import alias from:

```go
v1 "k8s.io/api/apps/v1"
```

to:

```go
appsv1 "k8s.io/api/apps/v1"
```

The helper code uses `appsv1.Deployment`.

- [ ] **Step 6: Add Deployment RBAC marker**

Add this marker under the existing CronScaleDemo RBAC markers:

```go
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
```

- [ ] **Step 7: Run controller tests and verify GREEN**

Run:

```bash
go test ./internal/controller
```

Expected: PASS.

- [ ] **Step 8: Run API test and verify it still passes**

Run:

```bash
go test ./api/v1 -run TestAPITypesDoNotUseAnonymousStructFields
```

Expected: PASS.

---

### Task 3: Regenerate Manifests and Verify Build-Relevant Commands

**Files:**
- Modify generated: `config/rbac/role.yaml`
- Possibly modify generated: `config/crd/bases/*.yaml`
- Test: repository commands

- [ ] **Step 1: Regenerate RBAC/manifests**

Run:

```bash
make manifests
```

Expected: command exits 0 and `config/rbac/role.yaml` includes deployment permissions from the new RBAC marker.

- [ ] **Step 2: Regenerate deepcopy code if API type changes are still pending**

Run:

```bash
make generate
```

Expected: command exits 0. If only controller code changed in this task, generated deepcopy code should not change.

- [ ] **Step 3: Run focused tests**

Run:

```bash
go test ./api/v1 -run TestAPITypesDoNotUseAnonymousStructFields && go test ./internal/controller
```

Expected: both commands exit 0.

- [ ] **Step 4: Run full Go tests**

Run:

```bash
go test ./...
```

Expected: exits 0 after the previous undefined symbol issue is resolved by this implementation path. If any e2e package fails because it requires an external cluster, report the exact package and error instead of claiming full success.

- [ ] **Step 5: Verify install target behavior**

Run:

```bash
make install
```

Expected: `controller-gen` does not panic. On this machine, this may still fail with `kubectl: command not found`; if it does, report that the controller-gen part is fixed and `kubectl` is the remaining environment dependency.

---

## Self-Review

**Spec coverage:** The plan covers minute-level time parsing, `defaultReplicas` outside the window, `replicas` inside the window, shared deployment scaling logic, logger fix, one-minute requeue, and Deployment RBAC.

**Placeholder scan:** No placeholders remain. Every code-changing step includes concrete code or a precise replacement instruction.

**Type consistency:** The plan consistently uses `DeploymentScaleTarget`, `minuteTimeLayout`, `appsv1.Deployment`, `SpecContext`, and existing `CronScaleDemoSpec` fields.
