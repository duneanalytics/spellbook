{{
  config(
    schema = 'polymarket_polygon',
    alias = 'capital_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time','evt_index','tx_hash'],
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
  'deposit' as action,
  "from" as from_address,
  to as to_address,
  symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC
  and to in (
    0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e, -- CTF Exchange
    0xc5d563a36ae78145c45a50134d48a1215220f80a  -- NegRisk CTF Exchange
  )
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}

union all

select
  block_time,
  block_number,
  'withdrawal' as action,
  "from" as from_address,
  to as to_address,
  symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC
  and "from" in (
    0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e, -- CTF Exchange
    0xc5d563a36ae78145c45a50134d48a1215220f80a  -- NegRisk CTF Exchange
  )
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}
