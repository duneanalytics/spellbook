{{
    config(
        schema = 'safe_berachain',
        alias= 'bera_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook = '{{ expose_spells(blockchains = \'["berachain"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["petertherock"]\') }}'
    )
}}

{% set project_start_date = '2024-10-01' %}

{{
    safe_native_transfers(
        blockchain = 'berachain',
        native_token_symbol = 'BERA',
        project_start_date = project_start_date
    )
}}
