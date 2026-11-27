# Xenny: notes for Claude

A streaming data platform on Kubernetes, run with GitOps. Start with [README.md](README.md) for the layout,
data flow and everyday commands; [docs/pipeline-walkthrough.md](docs/pipeline-walkthrough.md) follows a
file through every stage, and [docs/drills.md](docs/drills.md) is the failure-drill runbook.

## How changes ship

- ArgoCD syncs `main` from GitHub. A manifest change does nothing until it's pushed, and anything
  applied by hand to an ArgoCD-managed resource is reverted. To change a resource by hand
  (scale down, stop a connector), pause its app first. [docs/drills.md](docs/drills.md) has the
  `pause` and `resume` helpers.
- Never run `bootstrap/install.sh` while ArgoCD automation is paused.
- CI builds an image only when `apps/**`, `schemas/**`, `pom.xml`, `.mvn/**`, `.dockerignore` or the
  workflow changes. It then commits the new `sha-…` tag to the manifest, so pull before committing.
  Don't edit image tags by hand.
- I commit and push myself. Don't commit, push, or open PRs unless I ask.

## Checking your work

- Java (`apps/`, `schemas/`): `mvn -B verify`, which is the same command CI runs.
- Manifests: `kubectl apply --dry-run=server -f <file>` against the running cluster.
- Data: `tools/reconcile.sh` must print PASS after any pipeline change or drill.
- `tools/ports.sh <names>` opens port-forwards. Kill a forward by its PID, not with `pkill`,
  because I keep my own forwards open.

## Style

- Prefer the smallest change that works, and add to existing files rather than creating new ones.
- Comments say why, in one or two plain sentences. Match the tone of the existing YAML comments.
- Keep README.md and docs/ in step with any change to commands or behavior.
- Schemas evolve BACKWARD-compatibly: update consumers first, then producers. See drill 7.

## Gotchas

- The Iceberg sinks only append. Replaying a file adds it to bronze and silver again, so clear the
  downstream tables first (see "Replaying files" in the README).
- The cleaner drops any row it has already seen within its 1-day dedupe window, so a replay may
  never reach `clean.trips`. A full rebuild resets the cleaner too (drill 8).
- Kafka is three `dual` nodes (`xenny-dual-0..2`, each both controller and broker). The controller
  count can't change after the cluster is created.
- Don't `kubectl exec` into a pod that's near its memory limit, because the exec can get it
  OOMKilled. Check its usage with `kubectl top` first.
- I upload the trip files myself. Don't run ingests unless I ask.

## Secrets and private files

- Don't print the ntfy topic URL (secret `monitoring/alertmanager-ntfy`) or write it anywhere,
  since anyone with it can read the alerts. Keep it in a shell variable.
- The plan PDFs in the repo root are gitignored and stay local. Never commit them.
