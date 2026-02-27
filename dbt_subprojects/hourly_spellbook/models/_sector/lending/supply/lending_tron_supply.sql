{{
  config(
    schema = 'lending_tron',
    alias = 'supply',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with

supply_enriched as (
  {{
    lending_enrich_supply(
      model = ref('lending_tron_base_supply')
    )
  }}
)

select
  blockchain,
  project,
  version,
  transaction_type,
  symbol,
  to_tron_address(token_address) as token_address,
  to_tron_address(depositor) as depositor,
  to_tron_address(on_behalf_of) as on_behalf_of,
  to_tron_address(withdrawn_to) as withdrawn_to,
  to_tron_address(liquidator) as liquidator,
  amount,
  amount_raw,
  amount_usd,
  block_month,
  block_time,
  block_number,
  to_tron_address(project_contract_address) as project_contract_address,
  lower(to_hex(tx_hash)) as tx_hash,
  evt_index
from supply_enriched
