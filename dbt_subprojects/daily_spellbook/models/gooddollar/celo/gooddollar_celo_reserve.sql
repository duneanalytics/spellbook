{{
  config(
    schema = 'gooddollar_celo',
    alias = 'reserve',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'evt_index', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "gooddollar",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

reserve_flow as (
  select
    block_date,
    block_time,
    block_number,
    case
      when token_bought_symbol = 'G$' then 'inflow'
      when token_sold_symbol = 'G$' then 'outflow'
      else 'unknown'
    end as flow_type,
    token_sold_symbol as token_in_symbol,
    token_bought_symbol as token_out_symbol,
    token_sold_amount as token_in_amount,
    token_bought_amount as token_out_amount,
    token_sold_amount_raw as token_in_amount_raw,
    token_bought_amount_raw as token_out_amount_raw,
    amount_usd,
    token_sold_address as token_in_address,
    token_bought_address as token_out_address,
    taker,
    tx_from,
    tx_to,
    project_contract_address,
    evt_index,
    tx_hash
  from {{ source('dex', 'trades') }}
  where blockchain = 'celo'
    and project = 'gooddollar_reserve'
    and version = '4'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

reserve_funding as (
  select
    block_date,
    block_time,
    block_number,
    'funding' as flow_type,
    symbol as token_in_symbol,
    cast(null as varchar) as token_out_symbol,
    amount as token_in_amount,
    cast(null as double) as token_out_amount,
    amount_raw as token_in_amount_raw,
    cast(null as double) as token_out_amount_raw,
    amount_usd as token_in_amount_usd,
    contract_address as token_in_address,
    from_hex(null) as token_out_address,
    from_hex(null) as taker,
    "from" as tx_from,
    "to" as tx_to,
    from_hex('0x94a3240f484a04f5e3d524f528d02694c109463b') as project_contract_address,
    evt_index,
    tx_hash
  from {{ source('tokens', 'transfers') }}
  where blockchain = 'celo'
    and amount_raw > 0
    and ("to" = 0x94a3240f484a04f5e3d524f528d02694c109463b or "from" = 0x94a3240f484a04f5e3d524f528d02694c109463b)
    and (block_time, tx_hash) not in (select block_time, tx_hash from reserve_flow)
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
)

select * from reserve_funding
union all
select * from reserve_flow
