{{
    config(
        schema = 'safe_polygon',
        alias= 'matic_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(blockchains = \'["polygon"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke"]\') }}'
    )
}}

{% set project_start_date = '2021-03-07' %}

{{
    safe_native_transfers(
        blockchain = 'polygon',
        native_token_symbol = 'POL',
        project_start_date = project_start_date
    )
}}