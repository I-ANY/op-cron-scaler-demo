# op-scaler-demo
```bash
# 初始化项目
kubebuilder init --domain op-cron-scale-demo.example.com --repo github.com/example/op-cron-scale-demo --description "A sample cron scale operator"

# 创建API资源 GVK
kubebuilder create api --group api --version v1 --kind CronScaler
```
