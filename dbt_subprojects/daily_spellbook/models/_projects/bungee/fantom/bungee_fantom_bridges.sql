{{
    config(
        schema = 'bungee_fantom',
        alias = 'bridges',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['transfer_id']
    )
}}

{{ bungee_SocketBridge('fantom') }} 