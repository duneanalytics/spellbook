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
unique_trade_id
FROM
(SELECT * FROM {{ ref('opensea_ethereum_trades') }} 

                                UNION

SELECT blockchain,
project,
version,
block_time,
NULL::string as token_id,
NULL::string as collection,
amount_usd,
NULL::string as token_standard,
NULL::string as trade_type,
NULL::string as number_of_items,
NULL::string as trade_category,
evt_type,
NULL::string as seller,
NULL::string as buyer,
amount_original,
amount_raw,
currency_symbol,
currency_contract,
NULL::string as currency_contract_original,
NULL::string as nft_contract_address,
project_contract_address,
NULL::string as aggregator_name,
NULL::string as aggregator_address,
tx_hash,
NULL::string as tx_from,
NULL::string as tx_to,
unique_trade_id
FROM {{ ref('opensea_solana_trades') }}) 

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 

