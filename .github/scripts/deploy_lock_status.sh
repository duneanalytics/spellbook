#!/usr/bin/env bash
# Post the deploy-lock commit status for one PR.
# Usage: deploy_lock_status.sh <pr_number>
set -euo pipefail

PR="${1:?pr number required}"
STATE_LABEL="${STATE_LABEL:-deploy-lock-state}"
BYPASS_LABEL="${BYPASS_LABEL:-hotfix}"
CONTEXT="deploy-lock"

pr_json=$(gh pr view "$PR" --json headRefOid,labels,url)
sha=$(jq -r '.headRefOid' <<<"$pr_json")
url=$(jq -r '.url' <<<"$pr_json")
labels=$(jq -r '.labels[].name' <<<"$pr_json")

if [ -n "${STATE_JSON:-}" ]; then
  state="$STATE_JSON"
else
  state_num=$(gh issue list --label "$STATE_LABEL" --state open --json number --jq '.[0].number // empty')
  if [ -z "$state_num" ]; then
    state='{"locked":false}'
  else
    state=$(gh issue view "$state_num" --json body --jq '.body')
  fi
fi

locked=$(jq -r '.locked // false' <<<"$state" 2>/dev/null || echo false)

if [ "$locked" = 'true' ]; then
  expires=$(jq -r '.expires_at // empty' <<<"$state")
  if [ -n "$expires" ]; then
    now_ts=$(date -u +%s)
    exp_ts=$(python3 -c "import datetime,sys; print(int(datetime.datetime.strptime(sys.argv[1],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()))" "$expires")
    if [ "$now_ts" -ge "$exp_ts" ]; then
      locked=false
    fi
  fi
fi

post_status() {
  local st="$1" desc="$2"
  desc="${desc:0:140}"
  gh api "repos/${GH_REPO}/statuses/${sha}" \
    -f "state=${st}" \
    -f "context=${CONTEXT}" \
    -f "description=${desc}" \
    -f "target_url=${url}" >/dev/null
}

if [ "$locked" != 'true' ]; then
  post_status success 'No active deploy lock'
  exit 0
fi

if grep -qx "$BYPASS_LABEL" <<<"$labels"; then
  post_status success "hotfix label: bypassing lock"
  exit 0
fi

scope=$(jq -r '.scope' <<<"$state")
reason=$(jq -r '.reason // ""' <<<"$state")

in_scope=false
if [ "$scope" = 'all' ]; then
  in_scope=true
elif grep -qx "dbt: $scope" <<<"$labels"; then
  in_scope=true
fi

if [ "$in_scope" = 'true' ]; then
  post_status pending "Locked ($scope): $reason"
else
  post_status success "Lock active ($scope), PR not in scope"
fi
