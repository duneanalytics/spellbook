{{
  config(
    schema = 'gooddollar_celo',
    alias = 'ubi_claims',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "gooddollar",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

ubi_claimed as (
  select
    evt_block_time as block_time,
    date_trunc('minute', evt_block_time) as block_minute,
    cast(date_trunc('day', evt_block_time) as date) as block_date,
    evt_block_number as block_number,
    claimer,
    amount,
    contract_address as project_contract_address,
    evt_tx_hash as tx_hash
  from {{ source('gooddollar_celo', 'ubischemev2_evt_ubiclaimed') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

select
  uc.block_time,
  uc.block_date,
  uc.block_number,
  uc.claimer,
  uc.amount / power(10, coalesce(p.decimals, 18)) as amount,
  uc.amount / power(10, coalesce(p.decimals, 18)) * p.price as amount_usd,
  uc.amount as amount_raw,
  gf.gas_price,
  gf.gas_used,
  gf.tx_fee,
  gf.tx_fee_usd,
  gf.currency_symbol as tx_fee_currency_symbol,
  gf.tx_fee_currency,
  uc.project_contract_address,
  uc.tx_hash
from ubi_claimed uc
  left join {{ source('gas_celo', 'fees') }} gf on uc.block_time = gf.block_time
    and uc.block_number = gf.block_number
    and uc.tx_hash = gf.tx_hash
    {% if is_incremental() %}
    and {{ incremental_predicate('gf.block_time') }}
    {% endif %}
  left join {{ source('prices', 'minute') }} p on uc.block_minute = p.timestamp
    and p.contract_address = 0x62B8B11039FcfE5aB0C56E502b1C372A3d2a9c7A -- G$
    and p.blockchain = 'celo'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
