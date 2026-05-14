# Deploy lock

Manual guardrail for holding merges while someone debugs a prod issue. Not automated. Only works if people use it.

## When to use it

You're fixing a broken model or running a backfill and want a quiet window on `main` for a scope you care about. Lock the scope, fix, unlock. If you forget, the lock auto-releases after its TTL.

## Lock

1. Actions tab, pick **Deploy Lock**, run workflow.
2. Inputs:
   - `action`: `lock`
   - `scope`: one of `all`, `dex`, `daily`, `hourly`, `solana`, `tokens`, `shared`
   - `reason`: what you're doing. Shows in the PR check, Slack, and the pinned state issue.
   - `ttl_hours`: auto-release after N hours (default 4)
3. Confirm. The workflow updates the pinned Deploy Lock issue, posts to Slack, and refreshes `deploy-lock` on every open PR so stale statuses don't linger.

In-scope PRs show `deploy-lock` as pending. Merge button greys out.

## Unlock

Same workflow, `action: unlock`. Or do nothing. A cron releases expired locks every 15 minutes.

## Hotfix bypass

Add the `hotfix` label to the PR. `deploy-lock` flips to success and the PR is mergeable. Remove the label to re-block.

Use this when you're the lock holder fixing forward.

## Scope semantics

- `all` blocks every non-hotfix PR.
- Any other scope blocks PRs carrying the matching `dbt: <scope>` label (applied by the labeler based on changed paths). PRs outside that scope merge normally.
- `shared` covers `sources/**` and `dbt_macros/**`.

## State

Source of truth is a pinned GitHub issue labeled `deploy-lock-state`. Body is a JSON blob:

```json
{"locked": true, "scope": "dex", "reason": "fixing microbatch gap", "owner": "someone", "created_at": "...", "expires_at": "..."}
```

One lock at a time. Relocking overwrites.

## Admin bypass

Repo admins can bypass branch protection as usual. Use sparingly. The guardrail only works if people respect it.
