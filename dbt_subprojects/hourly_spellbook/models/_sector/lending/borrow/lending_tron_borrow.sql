{{
  config(
    schema = 'lending_tron',
    alias = 'borrow',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'transaction_type', 'token_address', 'tx_hash', 'evt_index', 'block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with

borrow_enriched as (
  {{
    lending_enrich_borrow(
      model = ref('lending_tron_base_borrow')
    )
  }}
)

select
  be.blockchain,
  be.project,
  be.version,
  be.transaction_type,
  be.loan_type,
  be.symbol,
  to_tron_address(be.token_address) as token_address,
  to_tron_address(be.borrower) as borrower,
  to_tron_address(be.on_behalf_of) as on_behalf_of,
  to_tron_address(be.repayer) as repayer,
  to_tron_address(be.liquidator) as liquidator,
  be.amount,
  be.amount_raw,
  be.amount_usd,
  be.block_month,
  be.block_time,
  be.block_number,
  to_tron_address(be.project_contract_address) as project_contract_address,
  lower(to_hex(be.tx_hash)) as tx_hash,
  be.evt_index
from borrow_enriched as be
