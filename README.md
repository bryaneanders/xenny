# xenny
Xenny is a modularized and extensible streaming data ingestion framework running on Kubernetes.

## Layout

```
bootstrap/        # cluster config, install script, root app-of-apps
bootstrap/apps/   # one ArgoCD Application per component
platform/         # shared infra config (Kafka, schema registry)
schemas/          # Avro schemas, one per registry subject; registered by a Job on sync
pipelines/trips/  # topics and deployments for the trips dataset
apps/ingest/      # ingest service: files in landing/trips/ -> raw.trips
```

ArgoCD syncs `main` from GitHub, so changes take effect once pushed.

## Dev cluster (MacBook)

Requires Docker, k3d, kubectl, helm, Java 25 and Maven.

```sh
k3d cluster create --config bootstrap/k3d-dev.yaml
./bootstrap/install.sh
```

`install.sh` works against any cluster in the current kube context (e.g. k3s on the Ubuntu box).
It creates the secrets kept out of git, installs ArgoCD, and applies the root app.

App images are built by CI and pushed to GHCR (`ghcr.io/bryaneanders/xenny-ingest`) on push to `main`.
CI then commits the new `sha-<commit>` tag to `pipelines/trips/ingest.yaml`, and ArgoCD deploys it.
Pull before your next commit to pick up that change.

## Ingesting a file

Upload a TLC trip file to `landing/trips/` (e.g. `yellow_tripdata_2026-07.parquet` from the
[TLC trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)).
With the AWS CLI and a port-forward:

```sh
kubectl -n lake port-forward svc/seaweedfs-all-in-one 8333 &
export AWS_ACCESS_KEY_ID=$(kubectl -n pipelines get secret s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
export AWS_SECRET_ACCESS_KEY=$(kubectl -n pipelines get secret s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
aws --endpoint-url http://localhost:8333 s3 cp yellow_tripdata_2026-07.parquet s3://landing/trips/
```

The ingest service moves finished files to `processed/trips/` and unreadable ones to `failed/trips/`.

## UIs

| UI | Port-forward | Login |
|---|---|---|
| ArgoCD | `kubectl -n argocd port-forward svc/argocd-server 8080:443` | `admin` / `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' \| base64 -d` |
| Grafana | `kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80` | `admin` / `kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' \| base64 -d` |
| Schema registry API | `kubectl -n kafka port-forward svc/apicurio-registry 8081:8080` | none; e.g. `curl localhost:8081/apis/ccompat/v7/subjects` |

S3 keys for SeaweedFS are in the `seaweedfs-s3` secret in the `lake` namespace.
