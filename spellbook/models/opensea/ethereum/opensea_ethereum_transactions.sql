{{ config(
        alias ='transactions',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}


SELECT blockchain,
project,
version,
block_time,
token_id,
collection,
amount_usd,
token_standard,
trade_type,
number_of_items,
trade_category,
evt_type,
seller,
buyer,
amount_original,
amount_raw,
currency_symbol,
currency_contract,
currency_contract_original,
nft_contract_address,
project_contract_address,
aggregator_name,
aggregator_address,
tx_hash,
tx_from,
tx_to,
fee_amount_raw,
fee_amount,
fee_amount_usd,
fee_receive_address,
fee_currency_symbol,
unique_trade_id
FROM ({{ ref('opensea_v1_ethereum_transactions') }})

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 