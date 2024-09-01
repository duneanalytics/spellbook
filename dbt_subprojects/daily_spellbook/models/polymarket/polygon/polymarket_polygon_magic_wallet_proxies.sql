{{
  config(
    schema = 'polymarket_polygon',
    alias = 'magic_wallet_proxies',
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
  block_time,
  block_number,
  'magic.link' as type_of_wallet,
  cast(null as varbinary) as owner,
  address as proxy,
  tx_hash
from {{ source('polygon', 'creation_traces') }}
where "from" = 0xaB45c5A4B0c941a2F231C04C3f49182e1A254052
{% if is_incremental() %}
  and {{ incremental_predicate('evt_block_time') }}
{% endif %}



