{{  config (
        alias = 'erc20_pools',
        partition_by = ['block_time'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        post_hook= '{{ expose_spells(\'["ethereum"]\',
                       "project", "Ajna",
                       \'["gunboats"]\'
                    )}}'

) }}

SELECT
  case
    when contract_address = 0x6146DD43C5622bB6D12A5240ab9CF4de14eDC625 then 9
    else 6 end as version
  collateral_ as collateral,
  quote_ as quote,
  pool_ as pool_address,
  cast(interestRate_ as decimal (38, 0)) / 1e18 as starting_interest_rate,
  call_tx_hash as tx_hash,
  call_block_time as block_time
FROM
  {{ source('ajna_ethereum', 'ERC20PoolFactory_call_deployPool')}}
{% if is_incremental() %}

    WHERE call_block_time >= date_trunc("day", now() - interval '1' day)

{% endif %}
JOIN
  (
    select
      pool_,
      evt_tx_hash
    from
      {{source('ajna_ethereum', 'ERC20PoolFactory_evt_PoolCreated')}}
  ) on call_tx_hash = evt_tx_hash
