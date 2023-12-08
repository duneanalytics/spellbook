{{  
    config(
        schema = 'oneinch',
        alias = 'calls_transfers_amounts',
        materialized = 'view',
        unique_key = ['blockchain', 'unique_call_transfer_id']
    )
}}



{% 
    set blockchains = [
        'arbitrum',
        'avalanche_c',
        'base',
        'bnb',
        'ethereum',
        'fantom',
        'gnosis',
        'optimism',
        'polygon',
        'zksync'
    ]
%}



{% for blockchain in blockchains %}
    select 
        blockchain
        , block_number
        , block_time
        -- tx
        , tx_hash
        , tx_from
        , tx_to
        , tx_success
        , tx_nonce
        , gas_price
        , priority_fee
        -- call
        , protocol
        , method
        , call_selector
        , call_success
        , call_trace_address
        , call_from
        , call_to
        , call_output 
        , call_error
        , call_gas_used
        , call_remains
        -- transfer
        , transfer_trace_address
        , contract_address
        , amount
        , native_token
        , transfer_from
        , transfer_to
        , transfers_between_players
        , rn_tta_asc
        , rn_tta_desc
        -- ext
        , minute
        , block_month
        , unique_call_transfer_id
    from {{ ref('oneinch_' + blockchain + '_calls_transfers') }}
    where 
        contract_address is not null
        and tx_success 
        and call_success

    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}