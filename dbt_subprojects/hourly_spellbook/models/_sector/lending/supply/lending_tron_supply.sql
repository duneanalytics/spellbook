{{
  config(
    schema = 'lending_tron',
    alias = 'supply',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'transaction_type', 'token_address', 'tx_hash', 'evt_index', 'block_month'],
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
  se.blockchain,
  se.project,
  se.version,
  se.transaction_type,
  se.symbol,
  to_tron_address(se.token_address) as token_address,
  to_tron_address(se.depositor) as depositor,
  to_tron_address(se.on_behalf_of) as on_behalf_of,
  to_tron_address(se.withdrawn_to) as withdrawn_to,
  to_tron_address(se.liquidator) as liquidator,
  se.amount,
  se.amount_raw,
  se.amount_usd,
  se.block_month,
  se.block_time,
  se.block_number,
  to_tron_address(se.project_contract_address) as project_contract_address,
  lower(to_hex(se.tx_hash)) as tx_hash,
  se.evt_index
from supply_enriched as se
