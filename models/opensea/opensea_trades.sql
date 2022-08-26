{{ config(
        alias ='trades'
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
nft_contract_address,
project_contract_address,
aggregator_name,
aggregator_address,
block_number,
tx_hash,
tx_from,
tx_to,
unique_trade_id
FROM {{ ref('opensea_ethereum_trades') }}

            UNION

SELECT blockchain,
project,
version,
block_time,
NULL::string as token_id,
NULL::string as collection,
amount_usd,
token_standard,
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
NULL::string as nft_contract_address,
project_contract_address,
NULL::string as aggregator_name,
NULL::string as aggregator_address,
block_number,
tx_hash,
NULL::string as tx_from,
NULL::string as tx_to,
unique_trade_id
FROM {{ ref('opensea_solana_trades') }}