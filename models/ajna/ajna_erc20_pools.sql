{{  config (
        schema = 'ajna',
        alias = 'erc20_pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool_address'],
        post_hook= '{{ expose_spells(\'["arbitrum", "base", "ethereum", "optimism", "polygon"]\',
                       "project", "ajna",
                       \'["gunboats"]\'
                    )}}'

) }}

{% set ajna_models = [
ref('ajna_arbitrum_erc20_pools')
, ref('ajna_optimism_erc20_pools')
, ref('ajna_ethereum_erc20_pools')
, ref('ajna_polygon_erc20_pools')
, ref('ajna_base_erc20_pools')
] %}

SELECT
  p.blockchain
  ,version
  ,collateral
  ,quote
  ,pool_address
  ,p.blockchain || '-rc' || cast(version as varchar) || '-' || coalesce(a.symbol, 'UNKNOWN') || '/' || coalesce(b.symbol, 'UNKNOWN') || '-' || cast(varbinary_substring(pool_address, 1, 6) as varchar) as name
  ,a.symbol as collateral_symbol
  ,b.symbol as quote_symbol
  ,a.decimals as collateral_decimal
  ,b.decimals as quote_decimal
  ,starting_interest_rate
  ,tx_hash
  ,block_time
  ,block_number
FROM (
    {% for i in ajna_models %}
      SELECT blockchain
      , version
      , collateral
      , quote
      , pool_address
      , starting_interest_rate
      , tx_hash
      , block_time
      , block_date
      , block_number
      FROM {{ i }}
      {% if is_incremental() %}
      WHERE {{ incremental_predicate('block_date') }}
      {% endif %}
      {% if not loop.last %}
      UNION ALL
      {% endif %}
      {% endfor %} 
) p
LEFT JOIN {{ source('tokens', 'erc20') }} a on a.contract_address = collateral and a.blockchain = p.blockchain
LEFT JOIN {{ source('tokens', 'erc20') }} b on b.contract_address = quote and b.blockchain = p.blockchain