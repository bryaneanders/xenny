# xenny
Xenny is a modularized and extensible streaming data ingestion framework running on Kubernetes.

## Layout

```
bootstrap/        # cluster config, install script, root app-of-apps
bootstrap/apps/   # one ArgoCD Application per platform component
platform/         # in-cluster config for those components (e.g. Kafka resources)
```

ArgoCD syncs `main` from GitHub, so changes take effect once pushed.

## Dev cluster (MacBook)

Requires Docker, k3d, kubectl and helm.

```sh
k3d cluster create --config bootstrap/k3d-dev.yaml
./bootstrap/install.sh
```

`install.sh` works against any cluster in the current kube context (e.g. k3s on the Ubuntu box).
It creates the secrets kept out of git, installs ArgoCD, and applies the root app.

## UIs

| UI | Port-forward | Login |
|---|---|---|
| ArgoCD | `kubectl -n argocd port-forward svc/argocd-server 8080:443` | `admin` / `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' \| base64 -d` |
| Grafana | `kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80` | `admin` / `kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' \| base64 -d` |

S3 keys for SeaweedFS are in the `seaweedfs-s3` secret in the `lake` namespace.
