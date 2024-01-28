with unit_test as (
  select
    case
      when from_hex(test.quote) = actual.quote then true
      else false
    end as quote_test,
    case
      when from_hex(test.collateral) = actual.collateral then true
      else false
    end as collateral_test,
    case
      when test.version = actual.version then true
      else false
    end as version_test
  from
    {{ ref('ajna_ethereum_erc20_pools')}} as actual
  inner join {{ ref('ajna_ethereum_erc20_pools_test_data')}} as test
  on
    actual.pool_address = from_hex(test.pool_address)
)

select
  *
from
  unit_test
where
  quote_test = false
  or collateral_test = false
  or version_test = false
  