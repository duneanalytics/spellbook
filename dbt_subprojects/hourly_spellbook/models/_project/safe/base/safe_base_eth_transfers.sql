{{ 
    safe_config(
        blockchain = 'base',
        alias_name = 'eth_transfers'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'base',
        native_token_symbol = 'ETH',
        project_start_date = '2023-07-01'
    )
}}
