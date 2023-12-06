{{
    config(
        schema = 'curvefi_celo',
        alias = 'pools',
        materialized = 'table',
        unique_key = ['version', 'tokenid', 'token', 'pool']
    )
}}

select
  'Base Pool' as version,
  cast(arg0 as int256) as tokenid,
  output_0 as token,
  contract_address as pool
from {{ source('curvefi_celo', 'StableSwap_call_coins') }}
where call_success
  and output_0 is not null
group by 2,3,4
