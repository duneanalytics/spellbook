{{ 
    config(
        materialized='incremental',
        schema = 'safe_bnb',
        alias = 'bnb_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(blockchains = \'["bnb"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke"]\') }}'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'bnb'
        , native_token_symbol = 'BNB'
        , project_start_date = '2021-01-26'
    )
}}
