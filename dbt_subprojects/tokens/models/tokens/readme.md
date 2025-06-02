## Tokens

This directory contains models for token metadata.

Token metadata is stored in `tokens.erc20` table in dune. We derive that token metadata directly from onchain calls to the token contract. 

Everything that lives in the models inside of this reposiotry are either historical artifacts from when we didn't automatcially pull metadata or manual overrides/additions. 

We will only accept contributions to this directory if they are:
- in their entirety not contained within the `tokens.erc20` table in dune
- strictly necessary for the project to function

If you need any additional token metadata for any non standard longtail tokens inside of Dune, consider just uploading the token metadata to a table in dune and joining that into your query instead.