{{
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "fantom", "optimism", "base", "zksync", "linea", "sonic", "unichain", "solana"]\',
                                "project",
                                "oneinch",
                                \'["max-morrow", "grkhr"]\') }}'
    )
}}

{% set evm_to_solana_mapping = {
    "blockchain" : "blockchain", 
    "block_number" : "block_slot",
    "block_time" : "block_time",
    "tx_hash" : "from_base58(tx_id)",
    "tx_from" : "from_base58(tx_signer)",
    "tx_to" : "from_base58(outer_executing_account)",
    "tx_nonce" : "null",
    "tx_gas_used" : "tx_gas_used",
    "tx_gas_price" : "tx_gas_price",
    "tx_priority_fee_per_gas" : "tx_priority_fee_per_gas",
    "contract_name" : "program_name",
    "protocol" : "null",
    "protocol_version" : "version",
    "method" : "method",
    "call_trace_address" : "call_trace_address",
    "call_from" : "from_base58(resolver)",
    "call_to" : "from_base58(outer_executing_account)",
    "call_gas_used" : "null",
    "call_type" : "null",
    "user" : "from_base58(user)",
    "receiver" : "from_base58(maker_receiver)",
    "order_hash" : "order_hash",
    "flags" : "map_from_entries(array[('contracts_only', false), ('cross_chain', false), ('direct', true), ('fusion', true), ('ordinary', null), ('second_side', false)])",
    "remains" : "null",
    "src_token_address" : "from_base58(src_token_mint)",
    "src_token_symbol" : "src_token_symbol",
    "src_token_decimals" : "src_token_decimals",
    "src_token_amount" : "src_token_amount",
    "src_escrow" : "null",
    "hashlock" : "null",
    "dst_blockchain" : "null",
    "dst_block_number" : "null",
    "dst_block_time" : "null",
    "dst_tx_hash" : "null",
    "dst_token_address" : "from_base58(dst_token_mint)",
    "dst_token_symbol" : "dst_token_symbol",
    "dst_token_decimals" : "dst_token_decimals", 
    "dst_token_amount" : "dst_token_amount",
    "amount_usd" : "amount_usd",
    "sources_amount_usd" : "sources_amount_usd",
    "src_token_amount_usd" : "src_token_amount_usd",
    "dst_token_amount_usd" : "dst_token_amount_usd",
    "transfers_amount_usd" : "amount_usd",
    "user_amount_usd" : "user_amount_usd",
    "tokens" : "tokens",
    "transfers" : "transfers",
    "escrow_results" : "null",
    "minute" : "date_trunc('minute', block_time)",
    "block_month" : "block_month",
    "swap_id" : "order_hash",
    "unique_key" : "unique_key",
} %}

select 
    {{ evm_to_solana_mapping.keys() | join('\n    , ') }}
from {{ ref('oneinch_evm_swaps') }}

union all

select 
    {{ evm_to_solana_mapping.values() | join('\n    , ') }}
from {{ source('oneinch_solana', 'swaps') }}