#!/usr/bin/env bash
# Port-forwards cluster UIs and APIs to localhost.
#   tools/ports.sh              list services, then choose which to open
#   tools/ports.sh all          open all of them
#   tools/ports.sh grafana s3   open only these
# Ctrl-C closes them all.
set -euo pipefail

# name | namespace | service | local:remote port | URL | what it is
SERVICES='
argocd     | argocd     | argocd-server                    | 8443:443  | https://localhost:8443                         | ArgoCD UI (accept the self-signed cert)
grafana    | monitoring | kube-prometheus-stack-grafana    | 3000:80   | http://localhost:3000                          | Grafana dashboards
prometheus | monitoring | kube-prometheus-stack-prometheus | 9090:9090 | http://localhost:9090                          | Prometheus queries and scrape targets
registry   | kafka      | apicurio-registry                | 8081:8080 | http://localhost:8081/apis/ccompat/v7/subjects | Schema registry API
connect    | kafka      | xenny-connect-api                | 8083:8083 | http://localhost:8083/connectors?expand=status | Kafka Connect API: connectors and their status
lakekeeper | lake       | lakekeeper                       | 8181:8181 | http://localhost:8181/ui                       | Iceberg catalog UI; REST catalog at /catalog
s3         | lake       | seaweedfs-all-in-one             | 8333:8333 | http://localhost:8333                          | S3 API for the aws CLI and DuckDB
filer      | lake       | seaweedfs-all-in-one             | 8888:8888 | http://localhost:8888/buckets/                 | SeaweedFS file browser for the buckets
'

rows() { grep -v '^$' <<<"$SERVICES" | sed 's/ *| */|/g'; }
row() { rows | grep "^$1|" || true; }

secret() { kubectl -n "$1" get secret "$2" -o jsonpath="{.data.$3}" 2>/dev/null | base64 -d || echo '?'; }
login() {
  case $1 in
    argocd) echo "admin / $(secret argocd argocd-initial-admin-secret password)" ;;
    grafana) echo "admin / $(secret monitoring grafana-admin admin-password)" ;;
    s3) echo "key $(secret pipelines s3-credentials AWS_ACCESS_KEY_ID), secret $(secret pipelines s3-credentials AWS_SECRET_ACCESS_KEY)" ;;
  esac
}

names=("$@")
if [ ${#names[@]} -eq 0 ]; then
  rows | while IFS='|' read -r name _ _ ports _ what; do
    printf '  %-11s localhost:%-6s %s\n' "$name" "${ports%%:*}" "$what"
  done
  read -rp "Open which? (names separated by spaces, or 'all'): " -a names
fi
[ "${names[*]:-}" = all ] && names=($(rows | cut -d'|' -f1))

# Ctrl-C or exit stops every forward started below.
trap 'trap - EXIT; kill 0' EXIT INT TERM

echo
for name in "${names[@]}"; do
  IFS='|' read -r _ ns svc ports url what <<<"$(row "$name")"
  local_port=${ports%%:*}
  if [ -z "$svc" ]; then
    echo "Unknown service: $name"; continue
  elif ! kubectl -n "$ns" get svc "$svc" >/dev/null 2>&1; then
    echo "Skipping $name: service $ns/$svc isn't running"; continue
  elif (echo >"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; then
    echo "Skipping $name: port $local_port is already in use"; continue
  fi
  # Restart the forward if it drops, e.g. when the pod restarts.
  (while true; do
    kubectl -n "$ns" port-forward "svc/$svc" "$ports" >/dev/null 2>&1 || true
    echo "$name: forward dropped, reconnecting"; sleep 2
  done) &
  printf '%-11s %-47s %s\n' "$name" "$url" "$what"
  creds=$(login "$name")
  [ -n "$creds" ] && printf '%-11s login: %s\n' '' "$creds"
done

echo
echo "Ctrl-C to close."
wait
