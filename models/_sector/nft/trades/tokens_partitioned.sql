{{ config(
    schema = 'nft',
    alias = 'tokens_partitioned',
    partition_by = ['contract'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['contract','token_id']
    )
}}



select
    contract,
    token_id,
    description as token_description,
    name as token_name,
    owner as current_token_owner,
from {{source('reservoir', 'tokens') }}