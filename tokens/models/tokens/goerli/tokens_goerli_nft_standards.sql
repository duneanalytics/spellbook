{{ config(
        alias = 'nft_standards',
        schema = 'tokens_goerli',
        materialized='incremental',
        incremental_strategy = 'merge',
        file_format = 'delta',
        unique_key = ['contract_address']
)
}}

 SELECT
  t.contract_address
, max_by(t.token_standard, t.block_time) AS standard
FROM {{ ref('nft_goerli_transfers') }} t
    {% if is_incremental() %}
       WHERE {{ incremental_predicate('t.block_time') }}
    {% endif %}
GROUP BY 1
