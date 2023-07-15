{{ config(
	tags=['legacy'],
        alias = alias('nft_standards', legacy_model=True),
        materialized='incremental',
        incremental_strategy = 'merge',
        file_format = 'delta',
        unique_key = ['contract_address']
)
}}

 SELECT
  t.contract_address
, max_by(t.token_standard, t.block_time) AS standard
FROM {{ ref('nft_avalanche_c_transfers_legacy') }} t
    {% if is_incremental() %}
       WHERE t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
GROUP BY 1
