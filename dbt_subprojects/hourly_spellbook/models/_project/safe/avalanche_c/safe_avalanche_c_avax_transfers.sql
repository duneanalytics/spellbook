{{ 
    safe_config(
        blockchain = 'avalanche_c',
        alias_name = 'avax_transfers'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'avalanche_c',
        native_token_symbol = 'AVAX',
        project_start_date = '2021-10-05',
        date_filter = true
    )
}}