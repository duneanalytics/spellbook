{{
    config(
        schema = 'zeroex_arc',
        alias = 'settler_addresses',
        materialized = 'incremental',
        unique_key = ['settler_address', 'token_id'],
        filtering_columns = ['settler_address'],
        on_schema_change = 'sync_all_columns',
        file_format = 'delta',
        incremental_strategy = 'merge'
    )
}}

{{ zeroex_settler_addresses('arc') }}
