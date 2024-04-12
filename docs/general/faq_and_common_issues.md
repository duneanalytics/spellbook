# Common issues overview

- Why is my PR running more models than included in the CI test runs?
    - Steps within the CI test workflow depend on DBT manifest file to be up-to-date on the main branch
    - When other PRs are merged, main is updated, therefore a new manifest file needs uploaded to storage for CI to read
    - The GH workflow to do this lives here: https://github.com/duneanalytics/spellbook/actions/workflows/commit_manifest.yml
    - If the latest run failed and/or is in progress, then it’s possible manifest files are out of date and your PR will run more than it should
    - Wait for it to complete or Dune team to fix any failures
- I’m modifying an existing spell within my PR that contains a seed test on it via the schema YML file. CI is failing on the seed test, as it can’t find the seed. How do I get around this?
    - We’ve recently setup seeds to run in prod, since CI defaults to prod if spells not in PR
    - Sometimes the seed doesn’t exist in prod (due to failure or other reason)
    - If the seed also isn’t in the PR, then CI test runs out of places to check
    - The workflow will fail on ‘metadata not found’ or ‘missing seed’
    - In order to get around this, force a change on the associated seed file and bring into PR to rebuild, then CI will be able to find in the test section
- I submitted a PR to add a new token to `prices.usd`, but it's still not showing up? Why is that?
    - new price additions can take a few days, as the backend process needs to acknowledge the new API ID value provided & backfill historical pricing data
    - please check back in a few days to see the new data populated