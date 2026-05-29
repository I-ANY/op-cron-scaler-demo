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
