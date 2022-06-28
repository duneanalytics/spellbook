{{ config(
        alias ='trades',
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
NULL::string as token_id,
NULL::string as collection,
amount_usd,
NULL::string as token_standard,
NULL::string as trade_type,
NULL::string as number_of_items,
NULL::string as trade_category,
NULL::string as evt_type,
NULL::string as seller,
NULL::string as buyer,
amount_original,
NULL::string as amount_raw,
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
NULL::string as fee_amount_raw,
NULL::double as fee_amount,
NULL::double as fee_amount_usd,
NULL::string as fee_receive_address,
NULL::string as fee_currency_symbol,
unique_trade_id
FROM {{ ref('magiceden_solana_trades') }}

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 