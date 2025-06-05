## Tokens

This directory contains models for token metadata.

Token metadata is stored in the `tokens.erc20` table in Dune. This data is derived directly from on-chain calls to token contracts and is generated outside of dbt. It combines data from individual chain models in the `tokens` subproject and our automated approach.

Models within this repository are primarily historical artifacts from before we automatically pulled metadata, or they consist of manual overrides and additions.

Contributions to this directory will only be accepted if they meet the following criteria:
- The data is not already present in Dune's `tokens.erc20` table.
- The data is strictly necessary for the project to function.

**A strong justification is required for adding new models to this directory; otherwise, pull requests will not be accepted.**

For additional metadata related to non-standard or long-tail tokens in Dune, consider uploading the metadata to a separate table in Dune and joining it into your queries.