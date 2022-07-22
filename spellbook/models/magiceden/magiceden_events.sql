{{ config(
        alias ='events',
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
NULL::string as collection,
amount_usd,
token_standard,
trade_type,
number_of_items,
NULL::string as trade_category,
evt_type,
seller,
buyer,
amount_original,
amount_raw,
currency_symbol,
currency_contract,
NULL::string as nft_contract_address,
project_contract_address,
NULL::string as aggregator_name,
NULL::string as aggregator_address,
tx_hash,
block_number,
NULL::string as tx_from,
NULL::string as tx_to,
platform_fee_amount_raw,
platform_fee_amount,
platform_fee_amount_usd,
platform_fee_percentage,
royalty_fee_amount_raw,
royalty_fee_amount,
royalty_fee_amount_usd,
royalty_fee_percentage,
NULL::string as royalty_fee_receive_address,
royalty_fee_currency_symbol,
unique_trade_id
FROM {{ ref('magiceden_solana_events') }}

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time >= (select max(block_time) from {{ this }})
{% endif %} 