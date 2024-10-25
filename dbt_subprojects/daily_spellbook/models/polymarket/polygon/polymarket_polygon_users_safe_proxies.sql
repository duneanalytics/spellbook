{{
  config(
    schema = 'polymarket_polygon',
    alias = 'users_safe_proxies',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['polymarket_wallet'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

select
  evt_block_time as block_time,
  evt_block_number as block_number,
  'safe' as type_of_wallet,
  owner,
  proxy as polymarket_wallet,
  evt_index,
  evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'SafeProxyFactory_evt_ProxyCreation') }}
{% if is_incremental() %}
where {{ incremental_predicate('evt_block_time') }}
{% endif %}
