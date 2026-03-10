{{
  config(
    schema = 'pumpswap_solana',
    alias = 'int_all_swaps',
    materialized = 'view'
  )
}}

SELECT
    block_slot
    , block_month
    , block_date
    , block_time
    , inner_instruction_index
    , swap_inner_index
    , outer_instruction_index
    , outer_executing_account
    , tx_id
    , tx_index
    , pool
    , user_account
    , account_user_base_token_account
    , account_user_quote_token_account
    , account_pool_base_token_account
    , account_pool_quote_token_account
    , account_protocol_fee_recipient_token_account
    , base_amount
    , quote_token_amount  
    , is_buy
    , surrogate_key
FROM {{ ref('pumpswap_solana_stg_decoded_swaps')}}

UNION ALL 

SELECT 
    block_slot
    , block_month
    , block_date
    , block_time
    , inner_instruction_index
    , swap_inner_index
    , outer_instruction_index
    , outer_executing_account
    , tx_id
    , tx_index
    , pool
    , user_account
    , account_user_base_token_account
    , account_user_quote_token_account
    , account_pool_base_token_account
    , account_pool_quote_token_account
    , account_protocol_fee_recipient_token_account
    , base_amount
    , quote_token_amount  
    , is_buy
    , surrogate_key
FROM {{ref('pumpswap_solana_stg_decoded_newevent')}}
