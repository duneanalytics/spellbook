{{
    config(
        schema = 'zeroex_robinhood',
        alias = 'settler_addresses',
        materialized = 'incremental',
        unique_key = ['settler_address', 'token_id'],
        on_schema_change = 'sync_all_columns',
        file_format = 'delta',
        incremental_strategy = 'merge'
    )
}}

{{ zeroex_settler_addresses('robinhood') }}
