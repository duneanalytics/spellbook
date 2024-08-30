{{
  config(
    schema = 'polymarket_polygon',
    alias = 'safe_proxies',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['proxy'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select
  evt_block_time as block_time,
  evt_block_number as block_number,
  owner,
  proxy,
  evt_index,
  evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'SafeProxyFactory_evt_ProxyCreation') }}
{% if is_incremental() %}
where {{ incremental_predicate('evt_block_time') }}
{% endif %}
