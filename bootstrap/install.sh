#!/usr/bin/env bash
# Bootstraps the current kube context: creates secrets kept out of git,
# installs ArgoCD, then hands everything else to the root app.
set -euo pipefail
cd "$(dirname "$0")"

ARGOCD_CHART_VERSION=10.11.1

for ns in argocd monitoring lake kafka; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

# Creates a secret only if it doesn't exist, so reruns keep existing credentials.
secret() {
  local ns=$1 name=$2; shift 2
  kubectl -n "$ns" get secret "$name" >/dev/null 2>&1 ||
    kubectl -n "$ns" create secret generic "$name" "$@"
}
rand() { openssl rand -hex "$1"; }

secret monitoring grafana-admin \
  --from-literal=admin-user=admin \
  --from-literal=admin-password="$(rand 16)"

secret lake seaweedfs-s3 \
  --from-literal=admin_access_key_id="$(rand 10)" \
  --from-literal=admin_secret_access_key="$(rand 20)" \
  --from-literal=read_access_key_id="$(rand 10)" \
  --from-literal=read_secret_access_key="$(rand 20)"

helm upgrade --install argocd argo-cd \
  --repo https://argoproj.github.io/argo-helm \
  --version "$ARGOCD_CHART_VERSION" \
  --namespace argocd \
  --set dex.enabled=false \
  --wait

kubectl apply -f root.yaml
