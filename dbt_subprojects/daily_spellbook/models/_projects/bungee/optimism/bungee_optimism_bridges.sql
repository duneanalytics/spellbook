{{
    config(
        schema = 'bungee_optimism',
        alias = 'bridges',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['transfer_id']
    )
}}

{{ bungee_SocketBridge('optimism') }} 