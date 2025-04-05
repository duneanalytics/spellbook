{{ 
    config(
        schema = 'safe_bnb',
        alias = 'bnb_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
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
