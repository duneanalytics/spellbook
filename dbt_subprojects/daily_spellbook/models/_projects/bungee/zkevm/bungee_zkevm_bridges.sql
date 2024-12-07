{{
    config(
        schema = 'bungee_zkevm',
        alias = 'bridges',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['transfer_id']
    )
}}

{{ bungee_SocketBridge('zkevm') }} 