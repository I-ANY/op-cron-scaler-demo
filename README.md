# op-scaler-demo
```bash
# 初始化项目
kubebuilder init --domain op-cron-scale-demo.example.com --repo github.com/example/op-cron-scale-demo --description "A sample cron scale operator"

# 创建API资源 GVK
kubebuilder create api --group api --version v1 --kind CronScaler
```
实现逻辑：
1、cron scaler 刚开始启动，首先保存需要更改副本数的目标 deployments 的原始副本数相关信息到 Annotations当中
2、判断时间范围，如果到范围内，进行扩展
3、恢复副本数：
  a. 过了时间范围，需要进行恢复deploy 的原来副本数
  b. cron scaler 资源删除也要进行恢复deploy 的原来副本数（通过Finalizer实现）

## 项目简介

op-scaler-demo 是一个基于 Kubebuilder 和 controller-runtime 实现的 Kubernetes Operator 示例项目。项目通过自定义资源 `CronScaler` 描述定时扩缩容规则，在控制器的 Reconcile 循环中根据当前时间调整目标 `Deployment` 的副本数。

该示例主要演示：

- 如何定义 Kubernetes CRD 和状态字段。
- 如何通过 controller-runtime 编写 Reconcile 逻辑。
- 如何使用 Finalizer 在资源删除前恢复目标 Deployment 的原始副本数。
- 如何在 `status` 中记录扩缩容失败的 Deployment，并通过 `kubectl get cronscaler` 展示失败摘要。

## 技术架构

| 模块 | 路径 | 说明 |
| --- | --- | --- |
| API 类型定义 | `api/v1/cronscaledemo_types.go` | 定义 `CronScaler` 的 `spec`、`status`、打印列和 CRD 校验规则。 |
| Controller 实现 | `internal/controller/cronscaledemo_controller.go` | 实现 Reconcile 主流程、Deployment 扩缩容、原始副本数保存和恢复逻辑。 |
| CRD/RBAC 配置 | `config/` | 存放 Kubebuilder 生成的 CRD、RBAC、manager 部署配置等 Kubernetes manifests。 |
| 入口程序 | `cmd/main.go` | 启动 controller manager，并注册 `CronScalerReconciler`。 |
| 自动生成代码 | `api/v1/zz_generated.deepcopy.go` | controller-gen 生成的 DeepCopy 方法，供 Kubernetes runtime 安全复制对象使用。 |
| 测试 | `internal/controller/cronscaledemo_controller_test.go` | 使用 fake client 验证 Reconcile 逻辑和状态更新逻辑。 |

整体架构如下：

```text
CronScaler CRD
     |
     v
controller-runtime Manager
     |
     v
CronScalerReconciler
     |
     +--> 读取 CronScaler spec
     +--> 读取和更新目标 Deployment
     +--> 写入 CronScaler status
     +--> 通过 Finalizer 处理删除前恢复
```

## CronScaler 资源说明

`CronScaler` 通过 `spec` 描述扩缩容目标和时间窗口：

| 字段 | 说明 |
| --- | --- |
| `spec.startTime` | 扩缩容开始时间，格式为 `HH:mm`。 |
| `spec.endTime` | 扩缩容结束时间，格式为 `HH:mm`。 |
| `spec.replicas` | 在时间窗口内希望调整到的 Deployment 副本数。 |
| `spec.deployments` | 需要扩缩容的 Deployment 列表，每个元素包含 `name` 和 `namespace`。 |

`CronScaler` 通过 `status` 展示当前处理结果：

| 字段 | 说明 |
| --- | --- |
| `status.status` | 当前状态，例如 `Running`、`Success`、`Failed`、`Restored`。 |
| `status.failedDeploymentSummary` | 扩缩容失败的 Deployment 摘要，用于 `kubectl get cronscaler` 展示，例如 `apps/web,apps/api`。 |
| `status.failedDeployments` | 扩缩容失败详情，用于 `kubectl get cronscaler <name> -o yaml` 查看失败原因、错误信息和失败时间。 |

## 当前实例处理逻辑

1. Reconcile 收到 `CronScaler` 事件后，先读取当前 `CronScaler` 对象。
2. 如果对象不存在，说明资源已被删除，直接忽略 NotFound 错误。
3. 如果对象未进入删除流程：
   - 确保资源带有 Finalizer。
   - 首次运行时将状态置为 `Running`。
   - 读取目标 Deployment 的原始副本数，并保存到 `CronScaler` 的 annotations 中。
   - 解析 `spec.startTime` 和 `spec.endTime`。
   - 判断当前时间是否处于扩缩容时间窗口内。
4. 如果当前时间在窗口内：
   - 遍历 `spec.deployments`。
   - 对每个 Deployment 执行副本数更新。
   - 如果某个 Deployment 更新失败，不影响其它 Deployment 继续处理。
   - 本轮处理完成后，统一更新 `status.status`、`status.failedDeploymentSummary` 和 `status.failedDeployments`。
5. 如果当前时间不在窗口内，并且之前已经扩缩容成功：
   - 从 annotations 中读取原始副本数。
   - 将目标 Deployment 恢复到原始副本数。
   - 将状态更新为 `Restored`。
6. 如果 `CronScaler` 正在删除：
   - 如果之前已经扩缩容成功，先恢复目标 Deployment 原始副本数。
   - 移除 Finalizer，让 Kubernetes 完成资源删除。

## 处理流程图

```text
┌──────────────────────┐
│   开始 Reconcile      │
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│   读取 CronScaler     │
└──────┬────────┬──────┘
       │        │
       │        └── NotFound ──> ┌──────────────┐
       │                         │  忽略并结束    │
       │                         └──────────────┘
       v
┌──────────────────────────────┐
│ CronScaler 是否正在删除？       │
└──────┬─────────────────┬─────┘
       │是               │否
       v                 v
┌──────────────────────┐   ┌──────────────────────┐
│ 状态是否为 Success？   │   │ 是否已有 Finalizer？   │
└──────┬────────┬──────┘   └──────┬────────┬──────┘
       │是      │否              │否      │是
       v       │                 v       │
┌──────────────────────┐   ┌──────────────────────┐
│ 恢复原始副本数          │   │ 添加 Finalizer       │
└──────────┬───────────┘   └──────────┬───────────┘
           │                          │
           v                          v
┌──────────────────────┐   ┌──────────────────────┐
│ 移除 Finalizer        │   │ 状态是否为空？         │
└──────────┬───────────┘   └──────┬────────┬──────┘
           │                      │是      │否
           v                      v       │
┌──────────────────────┐   ┌──────────────────────┐
│        结束           │   │ 状态置为 Running      │
└──────────────────────┘   └──────────┬───────────┘
                                      │
                                      v
                         ┌──────────────────────────────┐
                         │ 保存原始副本数到 annotations    │
                         └──────────┬───────────────────┘
                                    │
                                    v
                         ┌──────────────────────────────┐
                         │ 解析 startTime 和 endTime     │
                         └──────────┬───────────────────┘
                                    │
                                    v
                         ┌──────────────────────────────┐
                         │ 当前时间是否在扩缩容窗口内？      │
                         └──────┬─────────────────┬─────┘
                                │是               │否
                                v                 v
                  ┌──────────────────────┐   ┌──────────────────────┐
                  │ 遍历目标 Deployments   │   │ 状态是否为 Success？  │
                  └──────────┬───────────┘   └──────┬────────┬──────┘
                             │                      │是      │否
                             v                      v       v
                  ┌──────────────────────┐   ┌──────────────────────┐
                  │ scale 单个Deployment │   │ 恢复原始副本数       │
                  └──────┬────────┬──────┘   │ 状态置为 Restored    │
                         │成功    │失败      └──────────┬───────────┘
                         v       v                      │
                  ┌──────────┐  ┌──────────────────────┐ │
                  │ 继续下一个│  │ 记录失败详情和摘要   │ │
                  └────┬─────┘  └──────────┬───────────┘ │
                       │                   │             │
                       └─────────┬─────────┘             │
                                 v                       │
                  ┌──────────────────────┐               │
                  │ 是否还有 Deployment？│               │
                  └──────┬────────┬──────┘               │
                         │有      │无                    │
                         │       v                       │
                         │  ┌──────────────────────┐     │
                         │  │ 本轮是否存在失败？   │     │
                         │  └──────┬────────┬──────┘     │
                         │         │有      │无          │
                         │         v       v             │
                         │  ┌──────────┐ ┌──────────┐   │
                         │  │ 状态Failed│ │状态Success│   │
                         │  │ 写失败信息│ │清空失败记录│   │
                         │  └────┬─────┘ └────┬─────┘   │
                         │       │            │          │
                         └───────┴────────────┴──────────┘
                                         │
                                         v
                                  ┌──────────────┐
                                  │     结束     │
                                  └──────────────┘
```

## 失败展示方式

当部分 Deployment 扩缩容失败时，`kubectl get cronscaler` 可以通过 `FailedDeployments` 列快速查看失败对象：

```bash
kubectl get cronscaler
```

示例输出：

```text
NAME      STATUS   FAILEDDEPLOYMENTS   AGE
sample    Failed   apps/web,apps/api   5m
```

查看完整失败详情：

```bash
kubectl get cronscaler sample -o yaml
```

示例状态：

```yaml
status:
  status: Failed
  failedDeploymentSummary: apps/web
  failedDeployments:
    - name: web
      namespace: apps
      reason: UpdateDeploymentFailed
      message: 'deployments.apps "web" is forbidden: ...'
      lastTransitionTime: "2026-05-30T10:20:00Z"
```

## 常用命令

修改 `api/v1/*_types.go` 或 Kubebuilder marker 后，需要重新生成 CRD 和 DeepCopy 代码：

```bash
make manifests generate
```

验证 Go 语法和单元测试：

```bash
go test ./...
```
