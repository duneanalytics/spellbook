-- this tests checks a prices.tokens model against a tokens.erc20 model.
-- making sure we have the correct decimals for each token.
{% test test_prices_tokens_against_erc20(model) %}
With comparison as (

select 
  blockchain
  ,contract_address
  ,t.symbol as prices_symbol
  ,erc20.symbol as erc20_symbol
  ,(lower(t.symbol) = lower(erc20.symbol)) as equal_symbol 
  ,t.decimals as prices_decimals
  ,erc20.decimals as erc20_decimals
  ,(t.decimals = erc20.decimals) as equal_decimals
from {{ model }} t
inner join {{source('tokens','erc20')}} erc20 using (blockchain, contract_address)
)

select * from comparison
where not equal_decimals
{% endtest %}
